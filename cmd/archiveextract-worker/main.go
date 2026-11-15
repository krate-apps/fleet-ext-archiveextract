// Command archiveextract-worker extracts zip/rar archives via fleet.files.unpack (path-guarded host).
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      any             `json:"id"`
}

var (
	mu       sync.Mutex
	nextHost = int64(1)
	pending  = map[int64]chan hostReply{}
	outMu    sync.Mutex
)

type hostReply struct {
	result json.RawMessage
	err    string
}

func main() {
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for in.Scan() {
		line := in.Bytes()
		if len(line) == 0 {
			continue
		}
		var probe map[string]json.RawMessage
		if err := json.Unmarshal(line, &probe); err != nil {
			continue
		}
		if _, hasMethod := probe["method"]; !hasMethod {
			var resp struct {
				ID     any             `json:"id"`
				Result json.RawMessage `json:"result"`
				Error  *struct {
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(line, &resp); err != nil {
				continue
			}
			idNum, ok := asInt64(resp.ID)
			if !ok {
				continue
			}
			mu.Lock()
			ch := pending[idNum]
			delete(pending, idNum)
			mu.Unlock()
			if ch == nil {
				continue
			}
			msg := ""
			if resp.Error != nil {
				msg = resp.Error.Message
			}
			ch <- hostReply{result: resp.Result, err: msg}
			continue
		}

		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			continue
		}
		switch req.Method {
		case "fleet.handshake", "fleet.ping", "ext.activate", "ext.deactivate", "ext.health", "ext.upgrade":
			reply(req.ID, map[string]any{"ok": true})
		case "fleet.shutdown":
			reply(req.ID, map[string]any{"ok": true})
			return
		case "ext.archiveextract.run":
			var p struct {
				Archive string `json:"archive"`
				Dest    string `json:"dest"`
				Hash    string `json:"hash"`
				RelPath string `json:"relPath"`
			}
			if err := json.Unmarshal(req.Params, &p); err != nil {
				replyErr(req.ID, "invalid params")
				continue
			}
			archive := strings.TrimSpace(p.Archive)
			if archive == "" && p.Hash != "" && p.RelPath != "" {
				dir, err := torrentDir(p.Hash)
				if err != nil {
					replyErr(req.ID, err.Error())
					continue
				}
				archive = filepath.Join(dir, filepath.Clean(p.RelPath))
			}
			if archive == "" {
				replyErr(req.ID, "archive or hash+relPath required")
				continue
			}
			ext := strings.ToLower(filepath.Ext(archive))
			if ext != ".zip" && ext != ".rar" {
				replyErr(req.ID, "only .zip and .rar are supported")
				continue
			}
			res, err := hostCall("fleet.files.unpack", map[string]any{
				"archive": archive,
				"dest":    strings.TrimSpace(p.Dest),
			})
			if err != nil {
				replyErr(req.ID, err.Error())
				continue
			}
			reply(req.ID, json.RawMessage(res))
		default:
			replyErr(req.ID, fmt.Sprintf("method not found: %s", req.Method))
		}
	}
}

func torrentDir(hash string) (string, error) {
	raw, err := hostCall("fleet.torrents.list", map[string]any{})
	if err != nil {
		return "", err
	}
	var wrap struct {
		Torrents []struct {
			Hash      string `json:"hash"`
			Directory string `json:"directory"`
		} `json:"torrents"`
	}
	if err := json.Unmarshal(raw, &wrap); err != nil {
		return "", err
	}
	want := strings.ToUpper(strings.TrimSpace(hash))
	for _, t := range wrap.Torrents {
		if strings.ToUpper(t.Hash) == want {
			if strings.TrimSpace(t.Directory) == "" {
				return "", fmt.Errorf("torrent has empty directory")
			}
			return t.Directory, nil
		}
	}
	return "", fmt.Errorf("torrent not found")
}

func hostCall(method string, params any) (json.RawMessage, error) {
	mu.Lock()
	id := nextHost
	nextHost++
	ch := make(chan hostReply, 1)
	pending[id] = ch
	mu.Unlock()
	req := map[string]any{"jsonrpc": "2.0", "method": method, "params": params, "id": id}
	line, _ := json.Marshal(req)
	line = append(line, '\n')
	outMu.Lock()
	_, err := os.Stdout.Write(line)
	outMu.Unlock()
	if err != nil {
		return nil, err
	}
	select {
	case rep := <-ch:
		if rep.err != "" {
			return nil, fmt.Errorf("%s", rep.err)
		}
		if len(rep.result) == 0 {
			return json.RawMessage("null"), nil
		}
		return rep.result, nil
	case <-time.After(60 * time.Second):
		mu.Lock()
		delete(pending, id)
		mu.Unlock()
		return nil, fmt.Errorf("host call timeout")
	}
}

func reply(id any, result any) {
	outMu.Lock()
	defer outMu.Unlock()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func replyErr(id any, msg string) {
	outMu.Lock()
	defer outMu.Unlock()
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"error":   map[string]any{"code": -32000, "message": msg},
	})
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}
