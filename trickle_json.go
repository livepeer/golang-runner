package liverunner

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

type JSONWriter struct {
	publisher *TricklePublisher
}

func NewJSONWriter(p *TricklePublisher) *JSONWriter {
	return &JSONWriter{publisher: p}
}

func (w *JSONWriter) Write(msg any) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return w.publisher.Write(bytes.NewReader(data))
}

func (w *JSONWriter) Close() error {
	return w.publisher.Close()
}

type JSONReader struct {
	subscriber *TrickleSubscriber
}

func NewJSONReader(s *TrickleSubscriber) *JSONReader {
	return &JSONReader{subscriber: s}
}

func (r *JSONReader) Next() (map[string]any, error) {
	msg, _, err := r.next()
	return msg, err
}

// next returns the sequence to resume at, or a negative value if it is unknown.
func (r *JSONReader) next() (map[string]any, int, error) {
	body, nextSeq, err := readJSONSegment(r.subscriber)
	if err != nil || body == nil {
		return nil, nextSeq, err
	}
	var msg map[string]any
	if err := json.Unmarshal(body, &msg); err != nil {
		return nil, nextSeq, err
	}
	return msg, nextSeq, nil
}

type JSONLWriter struct {
	publisher *TricklePublisher
}

func NewJSONLWriter(p *TricklePublisher) *JSONLWriter {
	return &JSONLWriter{publisher: p}
}

func (w *JSONLWriter) Write(msg any) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return w.publisher.Write(bytes.NewReader(data))
}

func (w *JSONLWriter) Close() error {
	return w.publisher.Close()
}

type JSONLReader struct {
	subscriber *TrickleSubscriber
	pending    [][]byte
}

func NewJSONLReader(s *TrickleSubscriber) *JSONLReader {
	return &JSONLReader{subscriber: s}
}

func (r *JSONLReader) Next() (map[string]any, error) {
	for len(r.pending) == 0 {
		body, _, err := readJSONSegment(r.subscriber)
		if err != nil || body == nil {
			return nil, err
		}
		lines := bytes.Split(body, []byte{'\n'})
		for _, line := range lines {
			line = bytes.TrimSpace(line)
			if len(line) > 0 {
				r.pending = append(r.pending, append([]byte(nil), line...))
			}
		}
	}
	line := r.pending[0]
	r.pending = r.pending[1:]
	var msg map[string]any
	if err := json.Unmarshal(line, &msg); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, err
	}
	return msg, nil
}

func readJSONSegment(subscriber *TrickleSubscriber) ([]byte, int, error) {
	nextSeq := -1
	for attempts := 0; attempts < 6; attempts++ {
		resp, err := subscriber.Read()
		if err != nil {
			var missing *SequenceNonexistent
			if errors.As(err, &missing) {
				nextSeq = missing.Latest
				subscriber.SetSeq(nextSeq)
				continue
			}
			if errors.Is(err, EOS) || errors.Is(err, StreamNotFoundErr) {
				return nil, nextSeq, nil
			}
			return nil, nextSeq, err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if seq := GetSeq(resp); seq >= 0 {
			nextSeq = seq
			// Retry a partial body. Skip fully read segments even if their
			// JSON is malformed, since retrying would return the same data.
			if err == nil {
				nextSeq++
			}
		}
		return body, nextSeq, err
	}
	return nil, nextSeq, errors.New("trickle JSON reader exhausted sequence reset retries")
}
