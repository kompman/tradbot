package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"gonum.org/v1/gonum/stat"
)

// ===================== Настройки =====================
const (
	bybitREST        = "https://api.bybit.com"
	wsEndpoint       = "wss://stream.bybit.com/v5/public/spot"
	tickStoreSize    = 500               // размер буфера тиков
	minLiquidityUSD  = 10000.0           // минимум оборота для пары
	volatilityWeight = 0.6               // вес волатильности в score
	volumeWeight     = 0.4               // вес объёма в score
	volatilityInt    = 10 * time.Minute  // как часто пересчитываем пару
	saveInterval     = 5 * time.Second   // как часто публикуем/сохраняем тики
	apiUserAgent     = "AnalitikBot/1.0"
	wsDialTimeout    = 5 * time.Second
	wsReadTimeout    = 10 * time.Second
	apiRecvWindow    = "5000"

	natsURL      = "nats://localhost:4222"
	natsSubject  = "trading.data"
	dataFilePath = "trading_data.json"
)

// ===================== Конфигурация =====================
var cfg Config

type Config struct {
	APIKey       string
	APISecret    string
	FallbackPair string
	TestMode     bool
}

// ===================== Типы =====================
type MarketData struct {
	Turnover float64
	Volume   float64
	Last     float64
}

type Tick struct {
	Ts    int64   `json:"ts"`
	Price float64 `json:"price"`
	Qty   float64 `json:"qty"`
	Side  string  `json:"side"`
}

type TradingData struct {
	Symbol    string  `json:"symbol"`
	Price     float64 `json:"price"`
	VolPct    float64 `json:"vol_pct"`
	Timestamp int64   `json:"timestamp"`
	Ticks     []Tick  `json:"ticks,omitempty"`
}

// ===================== Глобальные переменные =====================
var (
	lg        *logrus.Logger
	logFile   *os.File
	natsConn  *nats.Conn
	rateLimit = make(chan struct{}, 40)

	tickBuf = make([]Tick, 0, tickStoreSize)
	bufMu   sync.Mutex

	// для публикации
	lastPrice float64
	lastVol   float64
)

// ===================== main =====================
func main() {
	printBanner()

	// читаем конфиг
	cfg = Config{
		APIKey:       "AmbrZFvlnJQnrG84mu",
		APISecret:    "24WTcfb3ODnBFXT0LGubleqA3dKCNtkZFIOC",
		FallbackPair: "PEPEUSDT",
		TestMode:     false,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	initLogging()
	defer logFile.Close()

	// NATS
	var err error
	natsConn, err = nats.Connect(natsURL,
		nats.MaxReconnects(5), nats.ReconnectWait(2*time.Second), nats.Timeout(5*time.Second))
	if err != nil {
		lg.Fatalf("Не удалось подключиться к NATS: %v", err)
	}
	defer natsConn.Close()
	lg.Info("Успешное соединение с NATS")

	// rate limit
	for i := 0; i < cap(rateLimit); i++ {
		rateLimit <- struct{}{}
	}

	var (
		wsConn      *websocket.Conn
		currentPair string
	)
	go func() {
		time.Sleep(100 * time.Millisecond) // чуть-чуть подождать, чтобы логгеры инициализировались
		volTicker.C <- time.Now()
	}()
	volTicker := time.NewTicker(volatilityInt)
	saveTicker := time.NewTicker(saveInterval)
	defer volTicker.Stop()
	defer saveTicker.Stop()

	lg.Infof("=== Цикл выбора пары каждые %s, публикация тиков каждые %s ===",
		volatilityInt, saveInterval)

	for {
		select {
		case <-ctx.Done():
			lg.Info("Завершаем работу")
			if wsConn != nil {
				wsConn.Close()
			}
			return

		case <-volTicker.C:
			// выбираем пару
			markets, err := getFilteredUSDTMarkets()
			if err != nil {
				lg.Errorf("Ошибка фильтрации пар: %v", err)
				continue
			}
			pair, vol := selectMostVolatilePair(markets, cfg.FallbackPair)
			price := markets[pair].Last
			lastPrice = price
			lastVol = vol
			printPairInfo(pair, price, vol)

			// переключаем WS-стрим при смене пары
			if pair != currentPair {
				if wsConn != nil {
					wsConn.Close()
				}
				bufMu.Lock()
				tickBuf = tickBuf[:0]
				bufMu.Unlock()

				wsConn, err = startTradeStream(pair)
				if err != nil {
					lg.Warnf("Ошибка WS для %s: %v", pair, err)
				}
				currentPair = pair
			}

		case <-saveTicker.C:
			// публикуем и сохраняем тики
			bufMu.Lock()
			ticks := make([]Tick, len(tickBuf))
			copy(ticks, tickBuf)
			bufMu.Unlock()

			if len(ticks) == 0 {
				continue
			}

			data := TradingData{
				Symbol:    currentPair,
				Price:     lastPrice,
				VolPct:    lastVol,
				Timestamp: time.Now().Unix(),
				Ticks:     ticks,
			}

			if err := natsConn.Publish(natsSubject, mustMarshal(data)); err != nil {
				lg.Errorf("Ошибка публикации в NATS: %v", err)
			}
			if err := saveTradingData(data); err != nil {
				lg.Errorf("Ошибка сохранения JSON: %v", err)
			}
			if err := saveHistory(currentPair, ticks); err != nil {
				lg.Errorf("Ошибка сохранения истории: %v", err)
			}
		}
	}
}

// ===================== Функции выбора пары =====================

func getFilteredUSDTMarkets() (map[string]MarketData, error) {
	url := fmt.Sprintf("%s/v5/market/tickers?category=spot", bybitREST)
	var resp struct {
		RetCode int `json:"retCode"`
		Result  struct{ List []struct {
			Symbol     string `json:"symbol"`
			Turnover24 string `json:"turnover24h"`
			Volume24   string `json:"volume24h"`
			LastPrice  string `json:"lastPrice"`
		}} `json:"result"`
	}
	if err := getJSON(url, &resp); err != nil {
		return nil, err
	}
	out := make(map[string]MarketData, len(resp.Result.List))
	for _, it := range resp.Result.List {
		if !strings.HasSuffix(it.Symbol, "USDT") {
			continue
		}
		to, _ := strconv.ParseFloat(it.Turnover24, 64)
		if to < minLiquidityUSD {
			continue
		}
		vol, _ := strconv.ParseFloat(it.Volume24, 64)
		last, _ := strconv.ParseFloat(it.LastPrice, 64)
		out[it.Symbol] = MarketData{Turnover: to, Volume: vol, Last: last}
	}
	return out, nil
}

func calcCombinedVolatility(symbol string) (float64, error) {
	// daily
	dailyURL := fmt.Sprintf("%s/v5/market/kline?category=spot&symbol=%s&interval=D&limit=100",
		bybitREST, symbol)
	var rd struct {
		RetCode int `json:"retCode"`
		Result  struct{ List [][]interface{} } `json:"result"`
	}
	if err := getJSON(dailyURL, &rd); err != nil {
		return 0, err
	}
	var closes []float64
	for _, k := range rd.Result.List {
		if s, ok := k[4].(string); ok {
			if p, err := strconv.ParseFloat(s, 64); err == nil {
				closes = append(closes, p)
			}
		}
	}
	if len(closes) < 2 {
		return 0, errors.New("недостаточно daily данных")
	}
	returns := make([]float64, len(closes)-1)
	for i := 1; i < len(closes); i++ {
		returns[i-1] = (closes[i] - closes[i-1]) / closes[i-1]
	}
	dailyVol := stat.StdDev(returns, nil) * math.Sqrt(365) * 100

	// hourly
	hourURL := fmt.Sprintf("%s/v5/market/kline?category=spot&symbol=%s&interval=60&limit=24",
		bybitREST, symbol)
	var rh struct {
		RetCode int `json:"retCode"`
		Result  struct{ List [][]interface{} } `json:"result"`
	}
	if err := getJSON(hourURL, &rh); err != nil {
		return dailyVol, nil
	}
	var highs, lows []float64
	for _, k := range rh.Result.List {
		if h, ok := k[2].(string); ok {
			if v, err := strconv.ParseFloat(h, 64); err == nil {
				highs = append(highs, v)
			}
		}
		if l, ok := k[3].(string); ok {
			if v, err := strconv.ParseFloat(l, 64); err == nil {
				lows = append(lows, v)
			}
		}
	}
	if len(highs) > 0 && len(lows) > 0 {
		minLow := lows[0]
		for _, v := range lows {
			if v < minLow {
				minLow = v
			}
		}
		maxHigh := highs[0]
		for _, v := range highs {
			if v > maxHigh {
				maxHigh = v
			}
		}
		shortVol := (maxHigh - minLow) / minLow * 100
		return volatilityWeight*dailyVol + (1-volatilityWeight)*shortVol, nil
	}
	return dailyVol, nil
}

func selectMostVolatilePair(data map[string]MarketData, fallback string) (string, float64) {
	type info struct{ Sym string; Score, Vol float64 }
	var list []info
	for sym, md := range data {
		vol, err := calcCombinedVolatility(sym)
		if err != nil || vol <= 0 {
			continue
		}
		score := volatilityWeight*vol + volumeWeight*math.Min(md.Volume/1e6, 40)
		list = append(list, info{sym, score, vol})
	}
	if len(list) == 0 {
		return fallback, 0
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Score > list[j].Score })
	return list[0].Sym, list[0].Vol
}

// ===================== WebSocket =====================

func startTradeStream(symbol string) (*websocket.Conn, error) {
	dialer := websocket.Dialer{
		HandshakeTimeout: wsDialTimeout,
		TLSClientConfig:  &tls.Config{InsecureSkipVerify: false},
	}
	conn, _, err := dialer.Dial(wsEndpoint, http.Header{"User-Agent": []string{apiUserAgent}})
	if err != nil {
		return nil, err
	}
	// подписываемся
	conn.WriteJSON(map[string]interface{}{
		"op":   "subscribe",
		"args": []string{fmt.Sprintf("publicTrade.%s", symbol)},
	})
	go func() {
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var m struct{ Data []struct {
				T int64   `json:"T"`
				P float64 `json:"p"`
				V float64 `json:"v"`
				S string  `json:"S"`
			}}
			if err := json.Unmarshal(raw, &m); err != nil {
				continue
			}
			bufMu.Lock()
			for _, tr := range m.Data {
				tickBuf = append(tickBuf, Tick{Ts: tr.T, Price: tr.P, Qty: tr.V, Side: tr.S})
				if len(tickBuf) > tickStoreSize {
					tickBuf = tickBuf[len(tickBuf)-tickStoreSize:]
				}
			}
			bufMu.Unlock()
		}
	}()
	return conn, nil
}

// ===================== HTTP + подпись =====================

func signRequest(req *http.Request) {
	if cfg.APIKey == "" {
		return
	}
	ts := strconv.FormatInt(time.Now().UnixNano()/1e6, 10)
	prehash := ts + req.Method + req.URL.RequestURI()
	mac := hmac.New(sha256.New, []byte(cfg.APISecret))
	mac.Write([]byte(prehash))
	sign := hex.EncodeToString(mac.Sum(nil))

	req.Header.Set("X-BAPI-API-KEY", cfg.APIKey)
	req.Header.Set("X-BAPI-TIMESTAMP", ts)
	req.Header.Set("X-BAPI-RECV-WINDOW", apiRecvWindow)
	req.Header.Set("X-BAPI-SIGN", sign)
}

func getJSON(url string, out interface{}) error {
	<-rateLimit
	defer func() { rateLimit <- struct{}{} }()

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", apiUserAgent)
	signRequest(req)

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// ===================== Сохранение данных =====================

func saveTradingData(data TradingData) error {
	f, err := os.Create(dataFilePath)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(data)
}

func saveHistory(symbol string, ticks []Tick) error {
	dir := filepath.Join("data", symbol)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	f, err := os.Create(filepath.Join(dir, time.Now().Format("2006-01-02")+".csv"))
	if err != nil {
		return err
	}
	defer f.Close()
	for _, t := range ticks {
		fmt.Fprintf(f, "%d,%.8f,%.8f,%s\n", t.Ts, t.Price, t.Qty, t.Side)
	}
	return nil
}

func mustMarshal(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// ===================== Логирование, баннер =====================

func initLogging() {
	lg = logrus.New()
	lg.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})
	if err := os.MkdirAll("logs", 0755); err != nil {
		log.Fatalf("Не удалось создать logs/: %v", err)
	}
	var err error
	logFile, err = os.OpenFile(filepath.Join("logs", time.Now().Format("2006-01-02")+".log"),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Не удалось открыть лог: %v", err)
	}
	lg.SetOutput(io.MultiWriter(os.Stdout, logFile))
}

func printBanner() {
	fmt.Println(`
██████╗ ██████╗ ███████╗ █████╗ ██╗     ██╗██╗  ██╗
██╔══██╗██╔══██╗██╔════╝██╔══██╗██║     ██║██║  ██║
██████╔╝██████╔╝█████╗  ███████║██║     ██║███████║
██╔═══╝ ██╔══██╗██╔══╝  ██╔══██║██║     ██║██╔══██║
██║     ██║  ██║███████╗██║  ██║███████╗██║██║  ██║
╚═╝     ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚══════╝╚═╝╚═╝  ╚═╝
[Система мониторинга запущена]
[Версия: 1.0]
`)
}

func printPairInfo(symbol string, price, vol float64) {
	fmt.Println("\n========================================")
	fmt.Printf("Выбранная пара: %s\n", symbol)
	fmt.Printf("Текущая цена: %.8f\n", price)
	fmt.Printf("Волатильность: %.2f%%\n", vol)
	fmt.Println("========================================\n")
}
