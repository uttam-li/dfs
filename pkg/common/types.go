package common

import "github.com/google/uuid"

type ChunkHandle struct {
	uuid.UUID
}

func NewChunkHandle() ChunkHandle {
	return ChunkHandle{uuid.New()}
}

func ParseChunkHandle(s string) (ChunkHandle, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return ChunkHandle{}, err
	}
	return ChunkHandle{id}, nil
}