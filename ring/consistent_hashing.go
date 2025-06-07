package ring

import (
	"fmt"
	"hash/crc32"
	"sort"
)

type Node struct {
	Hash   int
	server string
	data   []string
}

type HashRing []Node

func (h HashRing) Len() int {
	return len(h)
}
func (h HashRing) Less(i, j int) bool {
	return h[i].Hash < h[j].Hash
}

func (h HashRing) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *HashRing) AddServer(server string) {
	hash := crc32.ChecksumIEEE([]byte(server))
	node := Node{Hash: int(hash), server: server, data: []string{}}
	*h = append(*h, node)
	sort.Sort(h)

	idx := sort.Search(len(*h), func(i int) bool {
		return (*h)[i].Hash == int(hash)
	})
	idx = idx + 1
	if idx >= len(*h) {
		idx = 0
	}
	sd := []string{}
	sd = append(sd, (*h)[idx].data...)
	(*h)[idx].data = (*h)[idx].data[:0]
	for _, v := range sd {
		h.AddData(v)
	}
}
func (h *HashRing) RemoveServer(server string) {
	hash := crc32.ChecksumIEEE([]byte(server))
	for i, node := range *h {
		if node.Hash == int(hash) {
			next := i + 1
			if next >= len(*h) {
				next = 0
			}
			(*h)[next].data = append((*h)[next].data, node.data...)
			*h = append((*h)[:i], (*h)[i+1:]...)
			break
		}
	}
	sort.Sort(*h)
}

func (h *HashRing) AddData(data string) {
	hash := crc32.ChecksumIEEE([]byte(data))
	fmt.Println("DATA HASH: ", int(hash))

	idx := sort.Search(len(*h), func(i int) bool {
		return (*h)[i].Hash >= int(hash)
	})

	if idx == len(*h) {
		idx = 0
	}
	(*h)[idx].data = append((*h)[idx].data, data)
}
