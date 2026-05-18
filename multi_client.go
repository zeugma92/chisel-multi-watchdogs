package main

import (
 "fmt"
 "os"
 "time"
 "context"
 "flag"
 "gopkg.in/yaml.v3"
 "strings"
 "sync"

 "github.com/jpillora/chisel/share/cio"
)

var watchdogMu sync.Mutex
var watchdogStates = map[string]*TunnelWatchState{}

type TunnelWatchState struct {
 Name        string
 Cancel      context.CancelFunc
 LastBadTime time.Time
}

type TunnelConfig struct {
 Name   string`yaml:"name"`
 Remote string`yaml:"remote"`
}

type RangeTunnelConfig struct {
 NamePrefix string`yaml:"name_prefix"`
 From       int   `yaml:"from"`
 To         int   `yaml:"to"`

 Mode       string`yaml:"mode"`
 Bind       string`yaml:"bind"`

 TargetHost string`yaml:"target_host"`
 TargetFrom int   `yaml:"target_from"`
}

type WatchdogConfig struct {
 Enabled         bool     `yaml:"enabled"`
 ConnectTimeout  string   `yaml:"connect_timeout"`
 BadStateTimeout string   `yaml:"bad_state_timeout"`
 RestartDelay    string   `yaml:"restart_delay"`
 Patterns        []string `yaml:"patterns"`
}

type MultiConfig struct {
 Server           string             `yaml:"server"`
 Auth             string             `yaml:"auth"`
 TLSSkipVerify    bool               `yaml:"tls_skip_verify"`
 Fingerprint      string             `yaml:"fingerprint"`
 KeepAlive        string             `yaml:"keepalive"`
 MaxRetryInterval string             `yaml:"max_retry_interval"`
 MaxRetryCount    int                `yaml:"max_retry_count"`
 RetryDelay       string             `yaml:"retry_delay"`
 StartDelay	  string	     `yaml:"start_delay"`
 Tunnels          []TunnelConfig     `yaml:"tunnels"`
 RangeTunnels     []RangeTunnelConfig`yaml:"range_tunnels"`
 Watchdog 	  WatchdogConfig     `yaml:"watchdog"`
}


func multiClient(args []string) {
fs := flag.NewFlagSet("multi-client", flag.ExitOnError)
configPath := fs.String("config", "config.yaml", "path to config yaml")
configPathShort := fs.String("c", "", "path to config yaml")
fs.Parse(args)

if *configPathShort != "" {
 *configPath = *configPathShort
}

data, err := os.ReadFile(*configPath)
 if err != nil {
  panic(err)
 }

 var cfg MultiConfig
 if err := yaml.Unmarshal(data, &cfg); err != nil {
  panic(err)
 }

cio.LogHook = func(prefix string, line string) {
 if !cfg.Watchdog.Enabled {
  return
 }

 lowerLine := strings.ToLower(line)

 if strings.Contains(lowerLine, "connected") {
  watchdogMu.Lock()
  if state, ok := watchdogStates[prefix]; ok {
   state.LastBadTime = time.Time{}
  }
  watchdogMu.Unlock()
  return
 }

 matched := ""
 for _, pattern := range cfg.Watchdog.Patterns {
  if strings.Contains(lowerLine, strings.ToLower(pattern)) {
   matched = pattern
   break
  }
 }

 if matched == "" {
  return
 }

 timeout := 20 * time.Second
 if cfg.Watchdog.BadStateTimeout != "" {
  if d, err := time.ParseDuration(cfg.Watchdog.BadStateTimeout); err == nil {
   timeout = d
  }
 }

 watchdogMu.Lock()
 defer watchdogMu.Unlock()

 state, ok := watchdogStates[prefix]
 if !ok || state.Cancel == nil {
  return
 }

if state.LastBadTime.IsZero() {
 state.LastBadTime = time.Now()
 fmt.Printf("%s watchdog detected '%s', waiting %s before restart\n",
  prefix, matched, timeout)

 go func(prefix string, firstBad time.Time) {
  time.Sleep(timeout)

  watchdogMu.Lock()
  defer watchdogMu.Unlock()

  state, ok := watchdogStates[prefix]
  if !ok || state.Cancel == nil {
   return
  }

  if !state.LastBadTime.IsZero() && state.LastBadTime.Equal(firstBad) {
  fmt.Printf("%s watchdog timeout reached, restarting\n", prefix)
state.LastBadTime = time.Time{}
fmt.Printf("%s watchdog cancel sent\n", prefix)
state.Cancel()
  }
 }(prefix, state.LastBadTime)

 return
}

 if time.Since(state.LastBadTime) < timeout {
  return
 }

 fmt.Printf("%s watchdog timeout reached, restarting\n", prefix)
 state.LastBadTime = time.Time{}
 state.Cancel()
}

 allTunnels := buildAllTunnels(cfg)

 fmt.Println("Config:", *configPath)
 fmt.Println("Server:", cfg.Server)
 fmt.Println("Tunnels:", len(allTunnels))

startDelay := 0 * time.Second
if cfg.StartDelay != "" {
 if d, err := time.ParseDuration(cfg.StartDelay); err == nil {
  startDelay = d
 }
}

for _, t := range allTunnels {
 go runTunnel(cfg, t)

 if startDelay > 0 {
  time.Sleep(startDelay)
 }
}

 select {}
}

func buildAllTunnels(cfg MultiConfig) []TunnelConfig {
 all := make([]TunnelConfig, 0)

 all = append(all, cfg.Tunnels...)

 for _, r := range cfg.RangeTunnels {
  for port := r.From; port <= r.To; port++ {
   name := fmt.Sprintf("%s-%d", r.NamePrefix, port)

   var remote string

   if r.Mode == "socks" {
    if r.Bind != "" {
     remote = fmt.Sprintf("R:%s:%d:socks", r.Bind, port)
    } else {
     remote = fmt.Sprintf("R:%d:socks", port)
    }
   } else {
    targetPort := r.TargetFrom + (port - r.From)

    if r.Bind != "" {
     remote = fmt.Sprintf("R:%s:%d:%s:%d", r.Bind, port, r.TargetHost, targetPort)
    } else {
     remote = fmt.Sprintf("R:%d:%s:%d", port, r.TargetHost, targetPort)
    }
   }

   all = append(all, TunnelConfig{
    Name:   name,
    Remote: remote,
   })
  }
 }

 return all
}

func runTunnel(cfg MultiConfig, t TunnelConfig) {
 retryDelay := 5 * time.Second
 if cfg.RetryDelay != "" {
  if d, err := time.ParseDuration(cfg.RetryDelay); err == nil {
   retryDelay = d
  }
 }

 retryCount := 0

 for {
  fmt.Printf("[%s] starting -> %s\n", t.Name, t.Remote)

  args := []string{}

  args = append(args, "--log-prefix", "["+t.Name+"]")

  if cfg.Auth != "" {
   args = append(args, "--auth", cfg.Auth)
  }

  if cfg.TLSSkipVerify {
   args = append(args, "--tls-skip-verify")
  }

  if cfg.Fingerprint != "" {
   args = append(args, "--fingerprint", cfg.Fingerprint)
  }

  if cfg.KeepAlive != "" {
   args = append(args, "--keepalive", cfg.KeepAlive)
  }

  if cfg.MaxRetryInterval != "" {
   args = append(args, "--max-retry-interval", cfg.MaxRetryInterval)
  }

 args = append(args, cfg.Server, t.Remote)

ctx, cancel := context.WithCancel(context.Background())

prefix := "[" + t.Name + "]"

watchdogMu.Lock()
watchdogStates[prefix] = &TunnelWatchState{
 Name:   t.Name,
 Cancel: cancel,
}
watchdogMu.Unlock()

clientWithContext(ctx, args)

cancel()

watchdogMu.Lock()
delete(watchdogStates, prefix)
watchdogMu.Unlock()
  retryCount++

  if cfg.MaxRetryCount > 0 && retryCount >= cfg.MaxRetryCount {
   fmt.Printf("[%s] max retry count reached (%d), stopping\n", t.Name, cfg.MaxRetryCount)
   return
  }

  fmt.Printf("[%s] disconnected, retry in %s\n", t.Name, retryDelay)
  time.Sleep(retryDelay)
 }
}
