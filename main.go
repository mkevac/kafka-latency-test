package main

import (
	"flag"
	"fmt"

	"time"

	"log"

	"github.com/Shopify/sarama"
	"github.com/codahale/hdrhistogram"
	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
)

type result struct {
	count  uint64
	persec uint64
	q50    int64
	q75    int64
	q90    int64
	q99    int64
	q100   int64
}

func newResult(count, persec uint64, hist *hdrhistogram.Histogram) result {
	return result{
		count:  count,
		persec: persec,
		q50:    hist.ValueAtQuantile(50.0),
		q75:    hist.ValueAtQuantile(75.0),
		q90:    hist.ValueAtQuantile(90.0),
		q99:    hist.ValueAtQuantile(99.0),
		q100:   hist.ValueAtQuantile(100.0),
	}
}

func (r result) print() {
	fmt.Printf("count:\t%v\n", r.count)
	fmt.Printf("persec:\t%v\n", r.persec)
	fmt.Printf("percentile 50:\t%v\n", time.Duration(r.q50))
	fmt.Printf("percentile 75:\t%v\n", time.Duration(r.q75))
	fmt.Printf("percentile 90:\t%v\n", time.Duration(r.q90))
	fmt.Printf("percentile 99:\t%v\n", time.Duration(r.q99))
	fmt.Printf("percentile 100:\t%v\n", time.Duration(r.q100))
}

func startOptiopay() (chan result, error) {

	var (
		ch            = make(chan result)
		requests      = uint64(0)
		requestsSaved = uint64(0)
		hist          = hdrhistogram.New(time.Microsecond.Nanoseconds(), time.Minute.Nanoseconds(), 1)
		kv            = "foobar"
		messages      []*proto.Message
	)

	broker, err := kafka.Dial([]string{config.kafka}, kafka.NewBrokerConf("kafka-latency-test"))
	if err != nil {
		return nil, err
	}

	producerConf := kafka.NewProducerConf()
	producerConf.Compression = proto.CompressionSnappy
	producerConf.RequestTimeout = time.Second
	producerConf.RetryLimit = 1

	switch config.ack {
	case "none":
		producerConf.RequiredAcks = proto.RequiredAcksNone
	case "local":
		producerConf.RequiredAcks = proto.RequiredAcksLocal
	case "all":
		producerConf.RequiredAcks = proto.RequiredAcksAll
	}

	producer := broker.Producer(producerConf)

	for i := 0; i < 100; i++ {
		messages = append(messages, &proto.Message{Key: []byte(kv), Value: []byte(kv)})
	}

	go func() {
		b := time.Now()

		for {
			start := time.Now()

			if start.Sub(b) > config.updateInterval {
				persec := (requests - requestsSaved) / uint64(config.updateInterval.Seconds())
				ch <- newResult(requests, persec, hist)
				b = start
				start = time.Now()
				requestsSaved = requests
			}

			_, err := producer.Produce(config.topic, int32(config.partition), messages...)
			if err != nil {
				log.Printf("Error while producing to kafka: %s", err)
			}

			hist.RecordValue(time.Since(start).Nanoseconds())

			requests++
		}
	}()

	return ch, nil
}

func startSarama() (chan result, error) {

	var (
		ch            = make(chan result)
		requests      = uint64(0)
		requestsSaved = uint64(0)
		hist          = hdrhistogram.New(time.Microsecond.Nanoseconds(), time.Minute.Nanoseconds(), 1)
		kv            = "foobar"
		messages      []*sarama.ProducerMessage
	)

	for i := 0; i < 100; i++ {
		messages = append(messages, &sarama.ProducerMessage{
			Topic:     config.topic,
			Partition: int32(config.partition),
			Key:       sarama.ByteEncoder(kv),
			Value:     sarama.ByteEncoder(kv),
		})
	}

	saramaConfig := sarama.NewConfig()

	saramaConfig.Producer.Compression = sarama.CompressionSnappy // Compress messages
	saramaConfig.Producer.Return.Successes = true

	switch config.ack {
	case "none":
		saramaConfig.Producer.RequiredAcks = sarama.NoResponse
	case "local":
		saramaConfig.Producer.RequiredAcks = sarama.WaitForLocal
	case "all":
		saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	}

	producer, err := sarama.NewSyncProducer([]string{config.kafka}, saramaConfig)
	if err != nil {
		log.Fatalln("Failed to start Sarama producer:", err)
	}

	go func() {
	}()

	go func() {
		b := time.Now()

		for {
			start := time.Now()

			if start.Sub(b) > config.updateInterval {
				persec := (requests - requestsSaved) / uint64(config.updateInterval.Seconds())
				ch <- newResult(requests, persec, hist)
				b = start
				start = time.Now()
				requestsSaved = requests
			}

			if err := producer.SendMessages(messages); err != nil {
				log.Printf("Error while producing to kafka: %s", err)
			}

			hist.RecordValue(time.Since(start).Nanoseconds())

			requests++
		}
	}()

	return ch, nil
}

var config struct {
	kafka          string
	topic          string
	partition      uint
	updateInterval time.Duration
	client         string
	ack            string
}

func main() {
	flag.StringVar(&config.kafka, "kafka", "localhost", "kafka address")
	flag.StringVar(&config.topic, "topic", "latencytest", "kafka topic to use")
	flag.UintVar(&config.partition, "partition", 0, "kafka partition to use")
	flag.DurationVar(&config.updateInterval, "updateInterval", time.Second, "histogram update interval")
	flag.StringVar(&config.client, "client", "optiopay", "client name (sarama or optiopay)")
	flag.StringVar(&config.ack, "ack", "local", "ack waiting type (none or local or all)")
	flag.Parse()

	switch config.ack {
	case "none", "local", "all":
	default:
		log.Fatalf("Wrong ack type %v.", config.ack)
	}

	var (
		uptime = time.Now()
		histCh chan result
		err    error
	)

	if config.client == "optiopay" {

		log.Printf("Connected to kafka %s", config.kafka)

		histCh, err = startOptiopay()
		if err != nil {
			log.Fatalf("Error while starting optiopay: %s", err)
		}

	} else if config.client == "sarama" {

		histCh, err = startSarama()
		if err != nil {
			log.Fatalf("Error while starting sarama: %s", err)
		}

	} else {
		log.Fatalf("Wrong client")
	}

	for r := range histCh {
		fmt.Printf("Uptime: %v\n", time.Since(uptime))
		r.print()
	}
}
