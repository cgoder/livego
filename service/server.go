package service

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gwuhaolin/livego/av"
	"github.com/gwuhaolin/livego/protocol/rtmp/cache"
	"github.com/gwuhaolin/livego/protocol/rtmp/rtmprelay"

	log "github.com/sirupsen/logrus"
)

var (
	EmptyID = ""
)

type StreamType int

const (
	RtmpPush StreamType = iota
	RtmpPull
	RtspPull
	HlsPull
	HttpFlvPull
)

// 流媒体服务器。包含N个流媒体服务。
// 每个单独的流媒体服务都可以有多种输入流，多个输出流。
type StreamServer struct {
	services *sync.Map //key
}

// 创建流媒体服务器。并启动状态监测协程。
func NewStreamServers() *StreamServer {
	ss := &StreamServer{
		services: &sync.Map{},
	}
	go ss.CheckAlive(5)
	return ss
}

// 注册源流处理逻辑。并启动源流读取。
// 若已有源流，则重新启动；否则创建流媒体服务。
func (ss *StreamServer) HandleReader(r av.ReadCloser) {
	info := r.Info()
	log.Debugf("HandleReader: info[%v]", info)

	var service *StreamService
	// 查找是否存在此源流service的key。
	i, ok := ss.services.Load(info.Key)
	if service, ok = i.(*StreamService); ok {
		// 若已存在此源流key的service，则停止旧service。
		service.TransStop()
		// 旧源流ID是否与新源流ID一致，不一致则新建service，并替换旧service。
		id := service.ID()
		if id != EmptyID && id != info.UID {
			ns := NewStreamService()
			service.Copy(ns)
			service = ns
			ss.services.Store(info.Key, ns)
		} else {
			log.Debugf("renew service reader.")
		}
	} else {
		service = NewStreamService()
		ss.services.Store(info.Key, service)
		service.info = info
	}

	service.AddReader(r)
}

// 注册目标流处理逻辑。
// 若没有则新创建流媒体服务；若有则新增流媒体服务writer。
func (ss *StreamServer) HandleWriter(w av.WriteCloser) {
	info := w.Info()
	log.Debugf("HandleWriter: info[%v]", info)

	var service *StreamService
	item, ok := ss.services.Load(info.Key)
	if !ok {
		log.Debugf("HandleWriter: not found create new info[%v]", info)
		service = NewStreamService()
		ss.services.Store(info.Key, service)
		service.info = info
	} else {
		service = item.(*StreamService)
		service.AddWriter(w)
	}
}

// 获取所有流媒体服务
func (ss *StreamServer) GetServices() *sync.Map {
	return ss.services
}

// 定时遍历检测所有媒体服务状态。如果状态为关闭，则删除此服务。
func (ss *StreamServer) CheckAlive(ttl uint) {
	if ttl <= 1 {
		ttl = 1
	}

	d := time.Duration(ttl) * time.Second
	t := time.NewTimer(d)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			ss.services.Range(func(key, val interface{}) bool {
				v := val.(*StreamService)
				if v.CheckAlive() == 0 {
					ss.services.Delete(key)
				}
				return true
			})
			t.Reset(d)
			// default:
		}
	}
}

// 流媒体服务结构信息
type StreamService struct {
	stop  chan bool
	info  av.Info       // 源流信息
	cache *cache.Cache  // 源流视频数据缓冲区
	r     av.ReadCloser // 读源流handler
	ws    *sync.Map     // 推流目标地址集合。key为流UID，value为写流PackWriterCloser
}

// 写流数据
type PackWriterCloser struct {
	init bool
	w    av.WriteCloser // 写目标流handler
}

func (p *PackWriterCloser) GetWriter() av.WriteCloser {
	return p.w
}

// 实例化创建媒体服务。
func NewStreamService() *StreamService {
	return &StreamService{
		cache: cache.NewCache(),
		ws:    &sync.Map{},
	}
}

func (s *StreamService) ID() string {
	if s.r != nil {
		return s.r.Info().UID
	}
	return EmptyID
}

func (s *StreamService) GetReader() av.ReadCloser {
	return s.r
}

func (s *StreamService) GetWs() *sync.Map {
	return s.ws
}

// 复制流媒体服务。
func (s *StreamService) Copy(dst *StreamService) {
	dst.info = s.info
	s.ws.Range(func(key, val interface{}) bool {
		v := val.(*PackWriterCloser)
		s.ws.Delete(key)
		v.w.CalcBaseTimestamp()
		dst.AddWriter(v.w)
		return true
	})
}

// 新增源流处理。并启动读取流数据协程。
func (s *StreamService) AddReader(r av.ReadCloser) {
	s.r = r
	go s.TransStart()
}

func (s *StreamService) AddWriter(w av.WriteCloser) {
	info := w.Info()
	pw := &PackWriterCloser{w: w}
	s.ws.Store(info.UID, pw)
}

/*检测本application下是否配置static_push,
如果配置, 启动push远端的连接*/
func (s *StreamService) StartStaticPush() {
	key := s.info.Key

	dscr := strings.Split(key, "/")
	if len(dscr) < 1 {
		return
	}

	index := strings.Index(key, "/")
	if index < 0 {
		return
	}

	streamname := key[index+1:]
	appname := dscr[0]

	log.Debugf("StartStaticPush: current streamname=%s， appname=%s", streamname, appname)
	pushurllist, err := rtmprelay.GetStaticPushList(appname)
	if err != nil || len(pushurllist) < 1 {
		log.Debugf("StartStaticPush: GetStaticPushList error=%v", err)
		return
	}

	for _, pushurl := range pushurllist {
		pushurl := pushurl + "/" + streamname
		log.Debugf("StartStaticPush: static pushurl=%s", pushurl)

		staticpushObj := rtmprelay.GetAndCreateStaticPushObject(pushurl)
		if staticpushObj != nil {
			if err := staticpushObj.Start(); err != nil {
				log.Debugf("StartStaticPush: staticpushObj.Start %s error=%v", pushurl, err)
			} else {
				log.Debugf("StartStaticPush: staticpushObj.Start %s ok", pushurl)
			}
		} else {
			log.Debugf("StartStaticPush GetStaticPushObject %s error", pushurl)
		}
	}
}

func (s *StreamService) StopStaticPush() {
	key := s.info.Key

	log.Debugf("StopStaticPush......%s", key)
	dscr := strings.Split(key, "/")
	if len(dscr) < 1 {
		return
	}

	index := strings.Index(key, "/")
	if index < 0 {
		return
	}

	streamname := key[index+1:]
	appname := dscr[0]

	log.Debugf("StopStaticPush: current streamname=%s， appname=%s", streamname, appname)
	pushurllist, err := rtmprelay.GetStaticPushList(appname)
	if err != nil || len(pushurllist) < 1 {
		log.Debugf("StopStaticPush: GetStaticPushList error=%v", err)
		return
	}

	for _, pushurl := range pushurllist {
		pushurl := pushurl + "/" + streamname
		log.Debugf("StopStaticPush: static pushurl=%s", pushurl)

		staticpushObj, err := rtmprelay.GetStaticPushObject(pushurl)
		if (staticpushObj != nil) && (err == nil) {
			staticpushObj.Stop()
			rtmprelay.ReleaseStaticPushObject(pushurl)
			log.Debugf("StopStaticPush: staticpushObj.Stop %s ", pushurl)
		} else {
			log.Debugf("StopStaticPush GetStaticPushObject %s error", pushurl)
		}
	}
}

func (s *StreamService) IsSendStaticPush() bool {
	key := s.info.Key

	dscr := strings.Split(key, "/")
	if len(dscr) < 1 {
		return false
	}

	appname := dscr[0]

	//log.Debugf("SendStaticPush: current streamname=%s， appname=%s", streamname, appname)
	pushurllist, err := rtmprelay.GetStaticPushList(appname)
	if err != nil || len(pushurllist) < 1 {
		//log.Debugf("SendStaticPush: GetStaticPushList error=%v", err)
		return false
	}

	index := strings.Index(key, "/")
	if index < 0 {
		return false
	}

	streamname := key[index+1:]

	for _, pushurl := range pushurllist {
		pushurl := pushurl + "/" + streamname
		//log.Debugf("SendStaticPush: static pushurl=%s", pushurl)

		staticpushObj, err := rtmprelay.GetStaticPushObject(pushurl)
		if (staticpushObj != nil) && (err == nil) {
			return true
			//staticpushObj.WriteAvPacket(&packet)
			//log.Debugf("SendStaticPush: WriteAvPacket %s ", pushurl)
		} else {
			log.Debugf("SendStaticPush GetStaticPushObject %s error", pushurl)
		}
	}
	return false
}

func (s *StreamService) SendStaticPush(packet av.Packet) {
	key := s.info.Key

	dscr := strings.Split(key, "/")
	if len(dscr) < 1 {
		return
	}

	index := strings.Index(key, "/")
	if index < 0 {
		return
	}

	streamname := key[index+1:]
	appname := dscr[0]

	//log.Debugf("SendStaticPush: current streamname=%s， appname=%s", streamname, appname)
	pushurllist, err := rtmprelay.GetStaticPushList(appname)
	if err != nil || len(pushurllist) < 1 {
		//log.Debugf("SendStaticPush: GetStaticPushList error=%v", err)
		return
	}

	for _, pushurl := range pushurllist {
		pushurl := pushurl + "/" + streamname
		//log.Debugf("SendStaticPush: static pushurl=%s", pushurl)

		staticpushObj, err := rtmprelay.GetStaticPushObject(pushurl)
		if (staticpushObj != nil) && (err == nil) {
			staticpushObj.WriteAvPacket(&packet)
			//log.Debugf("SendStaticPush: WriteAvPacket %s ", pushurl)
		} else {
			log.Debugf("SendStaticPush GetStaticPushObject %s error", pushurl)
		}
	}
}

// 流媒体服务读取源流数据，并缓存，向所有目标流地址写数据。
func (s *StreamService) TransStart() {
	var p av.Packet

	log.Debugf("TransStart: %v", s.info)

	s.StartStaticPush()

	for {
		select {
		// 退出
		case <-s.stop:
			if s.r != nil {
				s.r.Close(fmt.Errorf("stop service"))
			}
			s.close()
			break
		// 正常读取源流并写目标流
		default:
			// 如果读取源流出错，则关闭此流媒体服务。
			err := s.r.Read(&p)
			if err != nil {
				log.Debugf("read source stream erro! stop service.")
				s.stop <- true
				continue
			}

			if s.IsSendStaticPush() {
				s.SendStaticPush(p)
			}

			// 缓存读取到的源流帧数据
			s.cache.Write(p)

			// 向每个目标流地址写源流帧数据
			s.ws.Range(func(key, val interface{}) bool {
				v := val.(*PackWriterCloser)
				if !v.init {
					log.Debugf("cache.send: %v", v.w.Info())
					if err = s.cache.Send(v.w); err != nil {
						log.Debugf("[%s] send cache packet error: %v, remove", v.w.Info(), err)
						s.ws.Delete(key)
						return true
					}
					v.init = true
				} else {
					newPacket := p
					writeType := reflect.TypeOf(v.w)
					log.Debugf("w.Write: type=%v, %v", writeType, v.w.Info())
					if err = v.w.Write(&newPacket); err != nil {
						log.Debugf("[%s] write packet error: %v, remove", v.w.Info(), err)
						s.ws.Delete(key)
					}
				}
				return true
			})
		}
	}
}

// 停止读取源流，并关闭流媒体服务。
func (s *StreamService) TransStop() {
	log.Debugf("TransStop: %s", s.info.Key)
	s.stop <- true
}

// 检测某个媒体服务状态
func (s *StreamService) CheckAlive() (n int) {
	if s.r != nil {
		if s.r.Alive() {
			n++
		} else {
			s.r.Close(fmt.Errorf("read timeout"))
		}
	}

	s.ws.Range(func(key, val interface{}) bool {
		v := val.(*PackWriterCloser)
		if v.w != nil {
			//Alive from RWBaser, check last frame now - timestamp, if > timeout then Remove it
			if !v.w.Alive() {
				log.Infof("write timeout remove")
				s.ws.Delete(key)
				v.w.Close(fmt.Errorf("write timeout"))
				return true
			}
			n++
		}
		return true
	})

	return
}

// 关闭流媒体服务。
func (s *StreamService) close() {
	if s.r != nil {
		s.StopStaticPush()
		log.Debugf("[%v] publisher closed", s.r.Info())
	}

	s.ws.Range(func(key, val interface{}) bool {
		v := val.(*PackWriterCloser)
		if v.w != nil {
			if v.w.Info().IsInterval() {
				v.w.Close(fmt.Errorf("closed"))
				s.ws.Delete(key)
				log.Debugf("[%v] player closed and remove\n", v.w.Info())
			}
		}
		return true
	})
}
