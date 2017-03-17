package bufferedQueue

import (
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

// BufferedQueue is a wrapper around an SQS queue that buffers messages and sends in batches.
type BufferedQueue interface {
	Send(msg *sqs.SendMessageInput)
	Close()
}

type bufferedQueueImpl struct {
	client    sqsiface.SQSAPI
	batchSize int
	queueURL  *string

	backgroundWorker sync.WaitGroup
	buffer           chan *sqs.SendMessageBatchRequestEntry
}

// NewBufferedQueue creates a new buffered queue for the given URL.
func NewBufferedQueue(svc sqsiface.SQSAPI, queueURL *string, batchSize int, bufferSize int) BufferedQueue {
	q := &bufferedQueueImpl{
		batchSize: batchSize,
		client:    svc,
		queueURL:  queueURL,
		buffer:    make(chan *sqs.SendMessageBatchRequestEntry, bufferSize),
	}

	q.start()

	return q
}

// start launches the background worker to process the queue of messages
func (q *bufferedQueueImpl) start() {
	q.backgroundWorker.Add(1)
	go func() {
		defer q.backgroundWorker.Done()
		runloop(q.buffer, q.batchSize, q.flushBatch)
	}()
}

func runloop(
	buffer <-chan *sqs.SendMessageBatchRequestEntry,
	batchSize int,
	flusher func(batch []*sqs.SendMessageBatchRequestEntry),
) {
	var counter int64
	for {
		items := make([]*sqs.SendMessageBatchRequestEntry, 0, batchSize)

		for i := 0; i < batchSize; i++ {
			item, ok := <-buffer

			if !ok {
				flusher(items)
				return
			}

			counter++
			item.Id = aws.String(strconv.FormatInt(counter, 10))
			items = append(items, item)
		}

		flusher(items)
	}
}

// Send enqueues the message to be sent to the queue. Send will block if the queue's buffer is full.
//
// Note: msg.QueueURL is ignored.
func (q *bufferedQueueImpl) Send(msg *sqs.SendMessageInput) {
	q.buffer <- &sqs.SendMessageBatchRequestEntry{
		DelaySeconds:           msg.DelaySeconds,
		MessageAttributes:      msg.MessageAttributes,
		MessageBody:            msg.MessageBody,
		MessageDeduplicationId: msg.MessageDeduplicationId,
		MessageGroupId:         msg.MessageGroupId,
	}
}

// Close sends all buffered messages to the queue and blocks until done.
func (q *bufferedQueueImpl) Close() {
	close(q.buffer)
	q.backgroundWorker.Wait()
}

func (q *bufferedQueueImpl) flushBatch(batch []*sqs.SendMessageBatchRequestEntry) {
	if len(batch) == 0 {
		return
	}

	_, err := q.client.SendMessageBatch(&sqs.SendMessageBatchInput{
		Entries:  batch,
		QueueUrl: q.queueURL,
	})

	if err != nil {
		err := err.(awserr.Error)

		if err.Code() == sqs.ErrCodeBatchRequestTooLong {
			if len(batch) > 1 {
				// large aggregate request, break it up
				midpoint := len(batch) / 2
				firstHalf := batch[:midpoint]
				secondHalf := batch[midpoint:]

				q.flushBatch(firstHalf)
				q.flushBatch(secondHalf)
			}
		}

		fmt.Fprintf(os.Stderr, "failed to flush messages (%v) with error %s\n", batch, err)
	}
}
