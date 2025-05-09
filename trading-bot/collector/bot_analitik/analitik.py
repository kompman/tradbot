import os
import time
import hmac
import hashlib
import logging
import json
import redis
import numpy as np
import ctypes
from datetime import datetime
from pybit.unified_trading import HTTP, WebSocket
from tenacity import retry, stop_after_attempt, wait_exponential

# Удержание окна консоли (Windows)
kernel32 = ctypes.windll.kernel32
kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)

# Конфигурация
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
API_KEY = "AmbrZFvlnJQnrG84mu"
API_SECRET = "24WTcfb3ODnBFXT0LGubleqA3dKCNtkZFIOC"
FALLBACK_PAIR = 'PEPEUSDT'
TEST_MODE = True
MIN_TURNOVER = 10000
TICKS_TO_STORE = 500
VOLATILITY_CHECK_INTERVAL = 600
EXCLUDED_PAIRS = []

# ASCII арт Casper
CASPER_ART = r"""
                                                    .^7JY5PPPPP5YJJJJ!^.                            
                                                 :?5PY7~^:..    ..:^!7JYYJ!:                        
                                               ^PBY~.                   :~?557.                     
                                             :5B?.                 ^^       ~JPY^                   
                                            ~#P:                 :7^^7~       .7PJ.                 
                     ~Y~~.                 ^@5                   ^^?J?Y7         J5:                
                   .^!P!^?7^              .BG                    .?:   .        . ^B7               
                   5Y~7G!.G??             7&:                        .~:       ^JJ !&!              
                  .P?~!:..Y.P!            5G                        !5?G~        Y: G5              
                   .!?!  :J.^P~           YP                       ~Y. !J     ~!.:. BJ              
                     .Y! .!  .!JJ!:       7#                       57  !7   :?!5?  7#:              
                      .!7?.    .^JP5!.    .P?                      B@~ J.  :5: !7 !&!               
                        .PG:       ~JPY~   :B7                     5B^7~:: Y@:.? 7#~                
                          JB~        .!PP7. :G7                    :!!~:Y~.BY^?^YB^                 
                           !G?          ^JPJ:.YY.                   .^:::77~~~ 5P.                  
                            :PP^          .!5Y~?5.           :::..       :^    B!                   
                              !G?.           ^JYPP:         .^^7#7             JG                   
                               .?P!            :7BB.           G@#7            ~B.                  
                                 :J5~             !?^         !@@@@5!^..       5P                   
                    .^!7?YJJ?7!^:  .7?~             :         :J5Y~..^^:     ^PB^                   
                .^?YPYJ?!!~!!?J555J7~!JJ!.                 ..            .^7G#Y.           .^!77??^ 
     .7YPPY~  ^JPY!:             .::::..~?7^               ^^!!!!!7777??JJJ77?7???7~^:. .~!7!!!~!77^
   :YGJ~^:!PJ?J!.                         :~~.                 !!..:::..         .^~~!~!!^. ^!!!B?7!
  !BJ.      :.                                               ~Y!                            ~J?77^. 
 !B~                                                      .!PG!::.                    .^!!~!7.      
:B^ .!                                                  ~JY?!7?JJYYJYYJJJ???777777?????7~^:.        
55  !5                                               ^J5Y~         ...::^^^^^^^^^::..               
#P. :GJ.                                         .~J55!.                                            
!P5YJ7?PJ!^^^^^^^!7JJ??7~:                  .:~?YYJ!.                                               
       .~?JJJJ?YBGYJ^^~ ..          ..:~!7JY55?~.                                                   
              !P^               :~7P#B5J?!~:.                                                       
             ?P.               .~YPY!:                                                              
            ~#:            .^75PY~.                                                                 
            J5    ~!!~~!7?Y5Y?~.                                                                    
            !B~. .~5J~~!~^:                                                                         
             !P5YY?^                                                                                
"""

# Настройка логов
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bybit_bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class BybitVolatilityBot:
    def __init__(self):
        print(CASPER_ART)
        logging.info("👻 CasperCryptoBot Activated")
        self.session = HTTP(
            api_key=API_KEY,
            api_secret=API_SECRET,
            testnet=TEST_MODE,
            recv_window=5000
        )
        self.ws = None
        self.redis = self._init_redis()
        self.current_pair = None
        self.price_history = []
        self.last_volatility_check = time.time()

    def _init_redis(self):
        """Инициализация Redis"""
        try:
            r = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                socket_connect_timeout=5
            )
            if r.ping():
                print("\n" + "="*50)
                print("✅ УСПЕШНОЕ ПОДКЛЮЧЕНИЕ К REDIS")
                print(f"• Хост: {REDIS_HOST}")
                print(f"• Порт: {REDIS_PORT}")
                print(f"• Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print("="*50 + "\n")
                logging.info(f"Redis подключен к {REDIS_HOST}:{REDIS_PORT}")
                return r
            logging.error("❌ Redis ping failed")
        except Exception as e:
            error_msg = f"🔴 Redis connection error: {e}"
            print(f"\n{error_msg}\n")
            logging.critical(error_msg)
        raise ConnectionError("Failed to connect to Redis")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def get_all_spot_pairs(self):
        """Получение спот-пар с фильтрацией"""
        try:
            response = self.session.get_tickers(category='spot')
            if response['retCode'] != 0:
                raise ValueError(f"API error: {response['retMsg']}")

            return {
                item['symbol']: {
                    'turnover': float(item['turnover24h']),
                    'volume': float(item['volume24h']),
                    'last_price': float(item['lastPrice'])
                }
                for item in response['result']['list']
                if (item['symbol'].endswith('USDT') and
                    float(item['turnover24h']) >= MIN_TURNOVER and
                    item['symbol'] not in EXCLUDED_PAIRS)
            }
        except Exception as e:
            logging.error(f"get_all_spot_pairs error: {e}")
            return {}

    def calculate_volatility(self, symbol):
        """Расчет комбинированной волатильности"""
        try:
            # Дневные данные
            daily = self.session.get_kline(
                category='spot',
                symbol=symbol,
                interval='D',
                limit=100
            )
            daily_closes = [float(c[4]) for c in daily['result']['list']]
            daily_returns = np.diff(daily_closes) / daily_closes[:-1]
            annual_vol = np.std(daily_returns) * np.sqrt(365) * 100

            # Часовые данные
            hourly = self.session.get_kline(
                category='spot',
                symbol=symbol,
                interval='60',
                limit=24
            )
            highs = [float(c[2]) for c in hourly['result']['list']]
            lows = [float(c[3]) for c in hourly['result']['list']]
            short_term_vol = (max(highs) - min(lows)) / min(lows) * 100

            return annual_vol * 0.7 + short_term_vol * 0.3
        except Exception as e:
            logging.error(f"Volatility calc error for {symbol}: {e}")
            return 0.0

    def select_most_volatile_pair(self):
        """Выбор пары по волатильности и объему"""
        pairs = self.get_all_spot_pairs()
        if not pairs:
            logging.warning(f"Using fallback pair: {FALLBACK_PAIR}")
            self.current_pair = FALLBACK_PAIR
            return

        scored = []
        for symbol, data in pairs.items():
            vol = self.calculate_volatility(symbol)
            if vol <= 0:
                continue

            score = vol * 0.6 + min(data['volume'] / 1_000_000, 40) * 0.4
            scored.append({
                'symbol': symbol,
                'score': score,
                'volatility': vol,
                'volume': data['volume']
            })

        if not scored:
            logging.warning(f"No valid pairs, using {FALLBACK_PAIR}")
            self.current_pair = FALLBACK_PAIR
            return

        best = max(scored, key=lambda x: x['score'])
        self.current_pair = best['symbol']
        logging.info(
            f"Selected: {best['symbol']} | "
            f"Score: {best['score']:.2f} | "
            f"Vol: {best['volatility']:.2f}% | "
            f"Volume: {best['volume']:,.0f} USDT"
        )

    def start_websocket(self):
        """Инициализация WebSocket"""
        try:
            if self.ws:
                self.ws.exit()

            self.ws = WebSocket(
                testnet=TEST_MODE,
                channel_type="spot",
                ping_interval=30,
                ping_timeout=10
            )
            
            self.ws.trade_stream(
                symbol=self.current_pair,
                callback=self.process_tick
            )
            
            logging.info(f"WebSocket connected to {self.current_pair}")
            return True
            
        except Exception as e:
            logging.error(f"WebSocket error: {e}")
            return False

    def process_tick(self, message):
        """Обработка тиковых данных"""
        try:
            if not message or 'data' not in message:
                return

            price = float(message['data'][0]['p'])
            self.price_history.append(price)
            if len(self.price_history) > TICKS_TO_STORE:
                self.price_history.pop(0)
        except Exception as e:
            logging.error(f"Tick processing error: {e}")

    def save_to_redis(self):
        """ИСПРАВЛЕННЫЙ МЕТОД: Сохранение данных в Redis с жесткой проверкой"""
        try:
            if not self.price_history:
                return

            # 1. Подготовка данных
            data = {
                'prices': self.price_history[-TICKS_TO_STORE:],
                'count': len(self.price_history),
                'timestamp': datetime.now().isoformat(),
                'volatility': self.calculate_volatility(self.current_pair)
            }
            key = f"ticks_{self.current_pair}"
            
            # 2. Транзакционная запись
            with self.redis.pipeline() as pipe:
                pipe.set(key, json.dumps(data), ex=3600*24)
                pipe.get(key)  # Немедленная проверка
                result = pipe.execute()
            
            # 3. Верификация
            if not result[0] or not result[1]:
                raise ValueError(f"Данные не подтверждены. SET: {result[0]}, GET: {result[1]}")
            
            logging.info(f"💾 УСПЕШНО: Ключ '{key}' | Тиков: {len(self.price_history)}")

        except Exception as e:
            logging.error(f"CRITICAL REDIS ERROR: {str(e)}")
            # Попытка восстановления соединения
            try:
                self.redis = self._init_redis()
                logging.info("Попытка переподключения к Redis")
            except Exception as reconnect_error:
                logging.critical(f"Не удалось восстановить соединение: {reconnect_error}")

    def run(self):
        """Основной цикл"""
        try:
            self.select_most_volatile_pair()
            if not self.start_websocket():
                raise ConnectionError("WebSocket failed")

            while True:
                now = time.time()
                
                if now - self.last_volatility_check >= VOLATILITY_CHECK_INTERVAL:
                    self.select_most_volatile_pair()
                    self.last_volatility_check = now

                if len(self.price_history) > 0:
                    self.save_to_redis()

                time.sleep(5)

        except KeyboardInterrupt:
            logging.info("Bot stopped by user")
        except Exception as e:
            logging.critical(f"Fatal error: {e}", exc_info=True)
        finally:
            if self.ws:
                self.ws.exit()
            logging.info("Bot shutdown complete")

if __name__ == '__main__':
    bot = BybitVolatilityBot()
    bot.run()