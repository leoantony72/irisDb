package engine

import (
	"fmt"
	"iris/config"
	"sort"
)

func FindNodeIDX(s *config.Server, hash uint16) int {
	fmt.Println("len:", len(s.Metadata))
	idx := sort.Search(len(s.Metadata), func(i int) bool {
		return s.Metadata[i].End >= hash
	})
	fmt.Println("hash:", hash, idx)

	return idx
}
