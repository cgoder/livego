package cache

import (
	"github.com/gwuhaolin/livego/av"
	"github.com/gwuhaolin/livego/configure"
	log "github.com/sirupsen/logrus"
)

type Cache struct {
	gop      *GopCache
	videoSeq *SpecialCache
	audioSeq *SpecialCache
	metadata *SpecialCache
}

func NewCache() *Cache {
	return &Cache{
		// 初始化GOP缓冲区，大小可配置
		gop:      NewGopCache(configure.Config.GetInt("gop_num")),
		videoSeq: NewSpecialCache(),
		audioSeq: NewSpecialCache(),
		metadata: NewSpecialCache(),
	}
}

// 源流数据帧写到缓存
func (cache *Cache) Write(p av.Packet) {
	// metadata
	if p.IsMetadata {
		cache.metadata.Write(&p)
		return
	} else {
		// audio
		if !p.IsVideo {
			ah, ok := p.Header.(av.AudioPacketHeader)
			if ok {
				if ah.SoundFormat() == av.SOUND_AAC &&
					ah.AACPacketType() == av.AAC_SEQHDR {
					cache.audioSeq.Write(&p)
					return
				} else {
					log.Debugf("unsupport audio fomat! ft:%v,acctype:%v\n", ah.SoundFormat(), ah.AACPacketType())
					return
				}
			}

		} else {
			// video
			vh, ok := p.Header.(av.VideoPacketHeader)
			if ok {
				if vh.IsSeq() {
					cache.videoSeq.Write(&p)
					return
				}
			} else {
				log.Debugf("unsupport video fomat!\n")
				return
			}

		}
	}
	// write gop
	cache.gop.Write(&p)
}

// 写流数据帧
func (cache *Cache) Send(w av.WriteCloser) error {
	if err := cache.metadata.Send(w); err != nil {
		return err
	}

	if err := cache.videoSeq.Send(w); err != nil {
		return err
	}

	if err := cache.audioSeq.Send(w); err != nil {
		return err
	}

	if err := cache.gop.Send(w); err != nil {
		return err
	}

	return nil
}
