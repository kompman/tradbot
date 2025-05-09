package main

import (
	"context"
	"crypto/tls"
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
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"gonum.org/v1/gonum/stat"
)

const (
	bybitREST       = "https://api.bybit.com"
	wsEndpoint      = "wss://stream.bybit.com/v5/public/spot"
	tickTarget      = 500
	volLookback     = 24 * time.Hour
	minLiquidityUSD = 10000
	minVolatility   = 0.8
	maxPairsToCheck = 20
	cooldownPeriod  = 5 * time.Minute

	wsConnectTimeout = 30 * time.Second
	wsReadTimeout    = 30 * time.Second
	wsWriteTimeout   = 30 * time.Second
	pingInterval     = 15 * time.Second

	apiUserAgent    = "AnalitikBot/1.0"
	apiRecvWindow   = "5000"
	rateLimitPerSec = 40
	dataFilePath    = "/home/k/kasper1711/trading_data.json"

	natsURL     = "nats://localhost:4222"
	natsSubject = "trading.data"
)

type Config struct {
	APIKey       string `yaml:"api_key"`
	APISecret    string `yaml:"api_secret"`
	FallbackPair string `yaml:"fallback_pair"`
	TestMode     bool   `yaml:"test_mode"`
}

type SymbolInfo struct {
	Symbol string
	Price  float64
	VolPct float64
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

var (
	lg        *logrus.Logger
	logFile   *os.File
	rateLimit = make(chan struct{}, rateLimitPerSec)
	wsCounter int
	natsConn  *nats.Conn
)

func main() {
	printBanner()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	initLogging()
	defer logFile.Close()

	// Подключение к NATS с улучшенными параметрами
	nc, err := nats.Connect(natsURL,
		nats.Timeout(5*time.Second),
		nats.MaxReconnects(5),
		nats.ReconnectWait(2*time.Second))
	if err != nil {
		lg.Fatalf("Не удалось подключиться к NATS: %v", err)
	}
	natsConn = nc
	defer natsConn.Close()
	lg.Info("Успешное соединение с NATS")

	if err := checkBybitConnection(); err != nil {
		printError("Подключение к Bybit", err)
		lg.Fatal(err)
	}
	lg.Info("Успешное соединение с биржей Bybit")

	for i := 0; i < rateLimitPerSec; i++ {
		rateLimit <- struct{}{}
	}

	cfg := Config{
		APIKey:       "AmbrZFvlnJQnrG84mu",
		APISecret:    "24WTcfb3ODnBFXT0LGubleqA3dKCNtkZFIOC",
		FallbackPair: "PEPEUSDT",
		TestMode:     false,
	}

	for {
		lg.Info("=== Поиск наиболее волатильной пары ===")
		if err := runOnce(ctx, cfg); err != nil {
			lg.Error(err)
		}
		printStep(fmt.Sprintf("Ожидание %v перед следующим циклом", cooldownPeriod))
		select {
		case <-ctx.Done():
			return
		case <-time.After(cooldownPeriod):
		}
	}
}

// Все остальные функции остаются БЕЗ ИЗМЕНЕНИЙ:

func printPairInfo(symbol string, price float64, vol float64) {
	fmt.Println("\n========================================")
	fmt.Printf("Выбранная пара: %s\n", symbol)
	fmt.Printf("Текущая цена: %.8f\n", price)
	fmt.Printf("Волатильность: %.2f%%\n", vol)
	fmt.Println("========================================\n")
}

func fetchUSDTMarketsSortedByVolume() ([]string, error) {
	url := fmt.Sprintf("%s/v5/market/tickers?category=spot", bybitREST)
	var resp struct {
		RetCode int `json:"retCode"`
		Result  struct {
			List []struct {
				Symbol    string `json:"symbol"`
				Volume24h string `json:"volume24h"`
			} `json:"list"`
		} `json:"result"`
	}
	if err := getJSON(url, &resp); err != nil {
		return nil, err
	}

	type pair struct {
		symbol string
		volume float64
	}
	var pairs []pair
	for _, item := range resp.Result.List {
		if strings.HasSuffix(item.Symbol, "USDT") {
			vol, err := strconv.ParseFloat(item.Volume24h, 64)
			if err != nil {
				continue
			}
			pairs = append(pairs, pair{item.Symbol, vol})
		}
	}

	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].volume > pairs[j].volume
	})

	result := make([]string, len(pairs))
	for i, p := range pairs {
		result[i] = p.symbol
	}
	return result, nil
}

func calcVolatility(symbol string) (float64, float64, error) {
	end := time.Now().UnixMilli()
	start := end - int64(volLookback/time.Millisecond)
	url := fmt.Sprintf("%s/v5/market/kline?category=spot&symbol=%s&interval=60&start=%d&end=%d",
		bybitREST, symbol, start, end)

	var resp struct {
		RetCode int `json:"retCode"`
		Result  struct {
			List [][]interface{} `json:"list"`
		} `json:"result"`
	}

	if err := getJSON(url, &resp); err != nil {
		return 0, 0, err
	}

	var prices []float64
	for _, kline := range resp.Result.List {
		if len(kline) < 5 {
			continue
		}
		closeStr, ok := kline[4].(string)
		if !ok {
			continue
		}
		price, err := strconv.ParseFloat(closeStr, 64)
		if err != nil {
			continue
		}
		prices = append(prices, price)
	}

	if len(prices) < 2 {
		return 0, 0, errors.New("недостаточно данных")
	}

	returns := make([]float64, len(prices)-1)
	for i := 1; i < len(prices); i++ {
		returns[i-1] = math.Log(prices[i] / prices[i-1])
	}

	stddev := stat.StdDev(returns, nil)
	volatility := stddev * math.Sqrt(24) * 100
	lastPrice := prices[len(prices)-1]

	return volatility, lastPrice, nil
}

func checkLiquidity(symbol string, threshold float64) (bool, error) {
	url := fmt.Sprintf("%s/v5/market/orderbook?category=spot&symbol=%s&limit=50",
		bybitREST, symbol)

	var resp struct {
		RetCode int `json:"retCode"`
		Result  struct {
			B [][2]string `json:"b"`
		} `json:"result"`
	}

	if err := getJSON(url, &resp); err != nil {
		return false, err
	}

	total := 0.0
	for _, bid := range resp.Result.B {
		price, _ := strconv.ParseFloat(bid[0], 64)
		size, _ := strconv.ParseFloat(bid[1], 64)
		total += price * size
	}

	return total >= threshold, nil
}

func pickMostVolatile(symbols []SymbolInfo) []SymbolInfo {
	sorted := make([]SymbolInfo, len(symbols))
	copy(sorted, symbols)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].VolPct > sorted[j].VolPct
	})
	return sorted
}

func collectTicksWithFallback(ctx context.Context, symbol string, limit int) ([]Tick, error) {
	ticks, err := collectTicksWithTimeout(ctx, symbol, limit)
	if err == nil && len(ticks) >= limit {
		return ticks, nil
	}

	lg.Warnf("WebSocket недоступен для %s, используем REST API", symbol)
	return fetchLastTradesREST(symbol, limit-len(ticks))
}

func collectTicksWithTimeout(ctx context.Context, symbol string, limit int) ([]Tick, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	dialer := websocket.Dialer{
		HandshakeTimeout:  wsConnectTimeout,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: false},
	}

	conn, _, err := dialer.Dial(wsEndpoint, http.Header{"User-Agent": []string{apiUserAgent}})
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	subMsg := map[string]interface{}{
		"op":   "subscribe",
		"args": []string{fmt.Sprintf("publicTrade.%s", symbol)},
	}

	if err := conn.WriteJSON(subMsg); err != nil {
		return nil, err
	}

	var ticks []Tick
	for len(ticks) < limit {
		select {
		case <-ctx.Done():
			return ticks, ctx.Err()
		default:
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return ticks, err
			}

			var data struct {
				Topic string `json:"topic"`
				Data  []struct {
					T int64   `json:"T"`
					P float64 `json:"p"`
					V float64 `json:"v"`
					S string  `json:"S"`
				} `json:"data"`
			}

			if err := json.Unmarshal(msg, &data); err != nil {
				continue
			}

			for _, trade := range data.Data {
				ticks = append(ticks, Tick{
					Ts:    trade.T,
					Price: trade.P,
					Qty:   trade.V,
					Side:  trade.S,
				})

				if len(ticks) >= limit {
					return ticks, nil
				}
			}
		}
	}
	return ticks, nil
}

func fetchLastTradesREST(symbol string, limit int) ([]Tick, error) {
	url := fmt.Sprintf("%s/v5/market/recent-trade?category=spot&symbol=%s&limit=%d", bybitREST, symbol, limit)
	
	var resp struct {
		RetCode int `json:"retCode"`
		Result  struct {
			List []struct {
				ExecPrice string `json:"execPrice"`
				ExecQty   string `json:"execQty"`
				ExecTime  string `json:"execTime"`
				Side      string `json:"side"`
			} `json:"list"`
		} `json:"result"`
	}

	if err := getJSON(url, &resp); err != nil {
		return nil, err
	}

	var ticks []Tick
	for _, trade := range resp.Result.List {
		price, _ := strconv.ParseFloat(trade.ExecPrice, 64)
		qty, _ := strconv.ParseFloat(trade.ExecQty, 64)
		ts, _ := strconv.ParseInt(trade.ExecTime, 10, 64)
		
		ticks = append(ticks, Tick{
			Ts:    ts,
			Price: price,
			Qty:   qty,
			Side:  trade.Side,
		})
	}

	return ticks, nil
}

func saveTradingData(data TradingData) error {
	file, err := os.Create(dataFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(data)
}

func saveHistory(symbol string, ticks []Tick) error {
	dir := filepath.Join("data", symbol)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	file, err := os.Create(filepath.Join(dir, time.Now().Format("2006-01-02")+".csv"))
	if err != nil {
		return err
	}
	defer file.Close()

	for _, tick := range ticks {
		_, err := fmt.Fprintf(file, "%d,%.8f,%.8f,%s\n", tick.Ts, tick.Price, tick.Qty, tick.Side)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkBybitConnection() error {
	resp, err := http.Get(bybitREST + "/v5/market/instruments-info?category=spot")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP status: %s", resp.Status)
	}
	return nil
}

func initLogging() {
	lg = logrus.New()
	lg.SetLevel(logrus.InfoLevel)
	lg.SetFormatter(&logrus.TextFormatter{FullTimestamp: true})

	logDir := "logs"
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatalf("Не удалось создать директорию логов: %v", err)
	}

	var err error
	logFile, err = os.OpenFile(
		filepath.Join(logDir, time.Now().Format("2006-01-02")+".log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0644,
	)
	if err != nil {
		log.Fatalf("Не удалось открыть файл лога: %v", err)
	}

	lg.SetOutput(io.MultiWriter(os.Stdout, logFile))
}

func getJSON(url string, target interface{}) error {
	<-rateLimit
	defer func() { rateLimit <- struct{}{} }()

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("User-Agent", apiUserAgent)
	client := &http.Client{Timeout: 10 * time.Second}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP status: %s", resp.Status)
	}

	return json.NewDecoder(resp.Body).Decode(target)
}

func printBanner() {
	fmt.Println(`
███████╗███████╗██████╗ ███████╗
██╔════╝██╔════╝██╔══██╗██╔════╝
███████╗█████╗  ██████╔╝███████╗
╚════██║██╔══╝  ██╔══██╗╚════██║
███████║███████╗██║  ██║███████║
╚══════╝╚══════╝╚═╝  ╚═╝╚══════╝
	`)
	fmt.Println("[Система мониторинга запущена]")
	fmt.Println("[Версия: 1.0]")
}

func printStep(msg string) {
	fmt.Printf("\n=== %s ===\n", msg)
}

func printSuccess(msg string) {
	fmt.Printf("\n✓ %s\n", msg)
}

func printError(ctx string, err error) {
	fmt.Printf("\n✗ Ошибка в %s: %v\n", ctx, err)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func runOnce(ctx context.Context, cfg Config) error {
	symbols, err := fetchUSDTMarketsSortedByVolume()
	if err != nil {
		return fmt.Errorf("ошибка получения списка пар: %w", err)
	}

	var validSymbols []SymbolInfo
	for _, symbol := range symbols[:min(len(symbols), maxPairsToCheck)] {
		volatility, price, err := calcVolatility(symbol)
		if err != nil {
			lg.Warnf("Ошибка расчета волатильности для %s: %v", symbol, err)
			continue
		}

		if volatility < minVolatility {
			continue
		}

		hasLiquidity, err := checkLiquidity(symbol, minLiquidityUSD)
		if err != nil {
			lg.Warnf("Ошибка проверки ликвидности для %s: %v", symbol, err)
			continue
		}

		if hasLiquidity {
			validSymbols = append(validSymbols, SymbolInfo{symbol, price, volatility})
		}
	}

	if len(validSymbols) == 0 {
		return errors.New("нет подходящих торговых пар")
	}

	selected := pickMostVolatile(validSymbols)[0]
	printPairInfo(selected.Symbol, selected.Price, selected.VolPct)

	ticks, err := collectTicksWithFallback(ctx, selected.Symbol, tickTarget)
	if err != nil {
		return fmt.Errorf("ошибка сбора тиков: %w", err)
	}

	data := TradingData{
		Symbol:    selected.Symbol,
		Price:     selected.Price,
		VolPct:    selected.VolPct,
		Timestamp: time.Now().Unix(),
		Ticks:     ticks,
	}

	if err := natsConn.Publish(natsSubject, mustMarshal(data)); err != nil {
		lg.Errorf("Ошибка публикации в NATS: %v", err)
	}

	if err := saveTradingData(data); err != nil {
		return fmt.Errorf("ошибка сохранения данных: %w", err)
	}

	if err := saveHistory(selected.Symbol, ticks); err != nil {
		lg.Errorf("Ошибка сохранения истории: %v", err)
	}

	printSuccess("Цикл завершен успешно")
	return nil
}

func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}