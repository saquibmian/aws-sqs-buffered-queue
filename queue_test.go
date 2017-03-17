package bufferedQueue

import (
	"fmt"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

var (
	mockQueueURL   = aws.String("fake-queue-URL")
	testBatchSize  = 10
	testBufferSize = 10000
)

type mockSqs struct {
	batchSize              int
	narrowMessageBatchSize int

	numberOfBatchesSent           int
	numberOfSendMessageBatchCalls int
	messagesSent                  []string

	sqsiface.SQSAPI
}

func (m *mockSqs) SendMessageBatch(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
	m.numberOfSendMessageBatchCalls++

	if len(input.Entries) > m.narrowMessageBatchSize {
		return nil, awserr.NewBatchError(sqs.ErrCodeBatchRequestTooLong, "", nil)
	}

	m.numberOfBatchesSent++
	for _, msg := range input.Entries {
		m.messagesSent = append(m.messagesSent, *msg.MessageBody)
	}
	return &sqs.SendMessageBatchOutput{}, nil
}

func (m *mockSqs) assertSendMessageBatchCalled(t *testing.T, times int) {
	if m.numberOfSendMessageBatchCalls != times {
		t.Errorf("expected SendMessageBatch to be called %d times, got %d", times, m.numberOfSendMessageBatchCalls)
	}
}

func (m *mockSqs) assertBatchesSent(t *testing.T, num int) {
	if m.numberOfBatchesSent != num {
		t.Errorf("expected %d batches sent, got %d", num, m.numberOfBatchesSent)
	}
}

func (m *mockSqs) assertMessagesSent(t *testing.T, msgs []string) {
	for i, expected := range msgs {
		actual := m.messagesSent[i]
		if expected != actual {
			t.Errorf("expected %dth message to be '%s', but was '%s'", i, expected, actual)
		}
	}
}

func createMockSqs() *mockSqs {
	mock := &mockSqs{
		batchSize:              testBatchSize,
		narrowMessageBatchSize: testBatchSize,
	}
	return mock
}

func createNarrowMessageMockSqs(narrowBatchSize int) *mockSqs {
	mock := &mockSqs{
		batchSize:              testBatchSize,
		narrowMessageBatchSize: narrowBatchSize,
	}
	return mock
}

func testWithQueue(t *testing.T, svc sqsiface.SQSAPI, msgs []string) {
	q := NewBufferedQueue(svc, mockQueueURL, testBatchSize, testBufferSize)

	for _, msg := range msgs {
		q.Send(&sqs.SendMessageInput{
			QueueUrl:    mockQueueURL,
			MessageBody: aws.String(msg),
		})
	}

	q.Close()
}

func Test_noMessages_noBatch(t *testing.T) {
	mock := createMockSqs()
	testWithQueue(t, mock, nil)

	mock.assertSendMessageBatchCalled(t, 0)
	mock.assertBatchesSent(t, 0)
}

func Test_partialBatch_sendsBatch(t *testing.T) {
	mock := createMockSqs()
	msgs := makeFakeMessages(mock.batchSize / 3)

	testWithQueue(t, mock, msgs)

	mock.assertSendMessageBatchCalled(t, 1)
	mock.assertBatchesSent(t, 1)
	mock.assertMessagesSent(t, msgs)
}

func Test_singleBatch_sendsBatch(t *testing.T) {
	mock := createMockSqs()
	msgs := makeFakeMessages(mock.batchSize)

	testWithQueue(t, mock, msgs)

	mock.assertSendMessageBatchCalled(t, 1)
	mock.assertBatchesSent(t, 1)
	mock.assertMessagesSent(t, msgs)
}

func Test_multipleBatches_sendsBatches(t *testing.T) {
	numBatches := 3
	mock := createMockSqs()
	msgs := makeFakeMessages(mock.batchSize * numBatches)

	testWithQueue(t, mock, msgs)

	mock.assertSendMessageBatchCalled(t, numBatches)
	mock.assertBatchesSent(t, numBatches)
	mock.assertMessagesSent(t, msgs)
}

func Test_largeMessagesFailure_sendsMultipleBatches(t *testing.T) {
	mock := createNarrowMessageMockSqs(2)
	msgs := makeFakeMessages(mock.batchSize)

	testWithQueue(t, mock, msgs)

	/* we expect this to happen
	   1 = 10 messages (fail)
	       2 = 5 messages (fail)
	           3 = 2 messages (pass)
	           4 = 3 messages (fail)
	               5 = 1 messages (pass)
	               6 = 2 messages (pass)
	       7 = 5 messages (fail)
	           8 = 2 messages (pass)
	           9 = 3 messages (fail)
	               10 = 1 messages (pass)
	               11 = 2 messages (pass)

	       therefore: 11 attempts, 6 successes
	*/
	mock.assertSendMessageBatchCalled(t, 11)
	mock.assertBatchesSent(t, 6)
	mock.assertMessagesSent(t, msgs)
}

func makeFakeMessages(num int) []string {
	var msgs []string
	for i := 0; i < num; i++ {
		msgs = append(msgs, fmt.Sprintf("%d", i))
	}
	return msgs
}

func BenchmarkSend(b *testing.B) {
	var (
		bufferSize = 10000
		batchSize  = 10
		maxRetries = 3
		queueURL   = os.Getenv("SQS_QUEUE_URL")
		message    = os.Getenv("MESSAGE")
	)

	sess, err := session.NewSession()
	if err != nil {
		b.Fatal(err)
	}

	svc := sqs.New(sess, aws.NewConfig().WithMaxRetries(maxRetries))
	queue := NewBufferedQueue(svc, aws.String(queueURL), batchSize, bufferSize)
	defer queue.Close()

	for n := 0; n < b.N; n++ {
		queue.Send(&sqs.SendMessageInput{MessageBody: &message})
	}
}
