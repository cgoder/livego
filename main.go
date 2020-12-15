package main

import (
	"fmt"
	"net"
	"path"
	"runtime"
	"time"

	"github.com/gwuhaolin/livego/configure"
	"github.com/gwuhaolin/livego/protocol/api"
	"github.com/gwuhaolin/livego/protocol/hls"
	"github.com/gwuhaolin/livego/protocol/httpflv"
	"github.com/gwuhaolin/livego/protocol/rtmp"
	"github.com/gwuhaolin/livego/service"

	log "github.com/sirupsen/logrus"
)

var VERSION = "master"

func HlsListenPull() *hls.Server {
	hlsAddr := configure.Config.GetString(configure.LISTEN_PORT_HLS)
	hlsListen, err := net.Listen("tcp", hlsAddr)
	if err != nil {
		log.Fatal(err)
	}

	hlsServer := hls.NewServer()
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("HLS server panic: ", r)
			}
		}()
		log.Info("HLS PULL listen On ", hlsAddr)
		hlsServer.Serve(hlsListen)
	}()
	return hlsServer
}

// var rtmpAddr string

func RtmpListenPush(server *service.StreamServer, hlsServer *hls.Server) {
	rtmpListenPort := configure.Config.GetString(configure.LISTEN_PORT_RTMP_PUSH)

	rtmpListen, err := net.Listen("tcp", rtmpListenPort)
	if err != nil {
		log.Fatal(err)
	}

	var rtmpServer *rtmp.Server

	if hlsServer == nil {
		rtmpServer = rtmp.NewRtmpServer(server, nil)
		log.Info("HLS server disable....")
	} else {
		rtmpServer = rtmp.NewRtmpServer(server, hlsServer)
		log.Info("HLS server enable....")
	}

	defer func() {
		if r := recover(); r != nil {
			log.Error("RTMP server panic: ", r)
		}
	}()
	log.Info("RTMP PUSH Listen On ", rtmpListenPort)
	rtmpServer.Serve(rtmpListen)
}

func ServerPull(streamType service.StreamType) {
	switch streamType {
	case service.RtmpPush:
		// TODO
	case service.RtmpPull:
		// TODO
	case service.RtspPull:
		// TODO
	case service.HlsPull:
		// TODO:
	case service.HttpFlvPull:
		// TODO:

	}
}

func HTTPFlvListenPull(server *service.StreamServer) {
	httpflvAddr := configure.Config.GetString(configure.LISTEN_PORT_HTTPFLV)

	flvListen, err := net.Listen("tcp", httpflvAddr)
	if err != nil {
		log.Fatal(err)
	}

	hdlServer := httpflv.NewServer(server)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("HTTP-FLV server panic: ", r)
			}
		}()
		log.Info("HTTP-FLV PULL listen On ", httpflvAddr)
		hdlServer.Serve(flvListen)
	}()
}

func startAPI(server *service.StreamServer) {
	apiAddr := configure.Config.GetString(configure.LISTEN_PORT_API)
	log.Info("HTTP-API listen port ", apiAddr)
	if apiAddr != "" {
		opListen, err := net.Listen("tcp", apiAddr)
		if err != nil {
			log.Fatal(err)
		}
		rtmpListenPort := configure.Config.GetString(configure.LISTEN_PORT_RTMP_PUSH)
		opServer := api.NewServer(server, rtmpListenPort)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Error("HTTP-API server panic: ", r)
				}
			}()
			log.Info("HTTP-API listen On ", apiAddr)
			opServer.Serve(opListen)
		}()
	} else {
		log.Info("HTTP-API listen port error")
	}
}

func init() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			filename := path.Base(f.File)
			return fmt.Sprintf("%s()", f.Function), fmt.Sprintf(" %s:%d", filename, f.Line)
		},
	})
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			log.Error("livego panic: ", r)
			time.Sleep(1 * time.Second)
		}
	}()

	log.Infof(`

                            █████                         
                           ░░███                          
  █████   ██████  ████████  ░███████    ██████  ████████  
 ███░░   ███░░███░░███░░███ ░███░░███  ███░░███░░███░░███ 
░░█████ ░███ ░███ ░███ ░███ ░███ ░███ ░███ ░███ ░███ ░███ 
 ░░░░███░███ ░███ ░███ ░███ ░███ ░███ ░███ ░███ ░███ ░███ 
 ██████ ░░██████  ░███████  ████ █████░░██████  ████ █████
░░░░░░   ░░░░░░   ░███░░░  ░░░░ ░░░░░  ░░░░░░  ░░░░ ░░░░░ 
                  ░███                                    
                  █████                                   
                 ░░░░░                                    	
        version: %s
	`, VERSION)

	// 创建流媒体服务器
	server := service.NewStreamServers()

	// 开启API服务
	startAPI(server)
	// 启动HLS写流服务
	hlsServer := HlsListenPull()
	// 启动http-flv写流服务
	HTTPFlvListenPull(server)
	// 启动rtmp收流服务
	RtmpListenPush(server, hlsServer)
	ServerPull(service.RtmpPush)
}
