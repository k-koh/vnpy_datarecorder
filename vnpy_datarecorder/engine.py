import traceback
from threading import Thread
from queue import Queue, Empty
from copy import copy
from collections import defaultdict
from datetime import datetime, timedelta, time

from vnpy.event import Event, EventEngine
from vnpy.trader.engine import BaseEngine, MainEngine
from vnpy.trader.constant import Exchange, Interval, Product
from vnpy.trader.object import (
    SubscribeRequest,
    TickData,
    BarData,
    ContractData
)
from vnpy.trader.event import EVENT_TICK, EVENT_CONTRACT, EVENT_TIMER
from vnpy.trader.utility import load_json, save_json, BarGenerator
from vnpy.trader.database import BaseDatabase, get_database, DB_TZ
from vnpy_spreadtrading.base import EVENT_SPREAD_DATA, SpreadItem


APP_NAME = "DataRecorder"

EVENT_RECORDER_LOG = "eRecorderLog"
EVENT_RECORDER_UPDATE = "eRecorderUpdate"


class RecorderEngine(BaseEngine):
    """
    For running data recorder.
    """

    setting_filename: str = "data_recorder_setting.json"

    def __init__(self, main_engine: MainEngine, event_engine: EventEngine) -> None:
        """"""
        super().__init__(main_engine, event_engine, APP_NAME)

        self.queue: Queue = Queue()
        self.thread: Thread = Thread(target=self.run)
        self.active: bool = False

        self.tick_recordings: dict[str, dict] = {}
        self.bar_recordings: dict[str, dict] = {}
        self.bar_generators: dict[str, BarGenerator] = {}

        self.timer_count: int = 0
        self.timer_interval: int = 10
        self.option_timer_count: int = 0
        self.option_timer_interval: int = 60 # Run every minute

        self.ticks: dict[str, list[TickData]] = defaultdict(list)
        self.bars: dict[str, list[BarData]] = defaultdict(list)

        self.filter_dt: datetime = datetime.now(DB_TZ)      # Tick数据过滤的时间戳
        self.filter_window: int = 60                        # Tick数据过滤的时间窗口，默认60秒
        self.filter_delta: timedelta                        # Tick数据过滤的时间偏差对象

        # 行情存活判定：休市日（假日）网关只在启动时推送一次陈旧的快照Tick，
        # 之后不再有Tick。记录最近一次有效Tick的本地时间，若超过存活窗口未
        # 收到有效Tick，则不保存K线与期权数据。
        self.last_tick_dt: datetime | None = None
        self.feed_alive_window: int = 120                   # 行情存活时间窗口，默认120秒

        self.database: BaseDatabase = get_database()

        self.load_setting()
        self.register_event()
        self.start()
        self.put_event()

    def load_setting(self) -> None:
        """"""
        setting: dict = load_json(self.setting_filename)
        self.tick_recordings = setting.get("tick", {})
        self.bar_recordings = setting.get("bar", {})

        self.filter_window = setting.get("filter_window", 60)
        self.filter_delta = timedelta(seconds=self.filter_window)

        self.feed_alive_window = setting.get("feed_alive_window", 120)

    def save_setting(self) -> None:
        """"""
        setting: dict = {
            "tick": self.tick_recordings,
            "bar": self.bar_recordings
        }
        save_json(self.setting_filename, setting)

    def run(self) -> None:
        """"""
        while self.active:
            try:
                task: tuple[str, list] = self.queue.get(timeout=1)
                task_type, data = task

                if task_type == "tick":
                    self.database.save_tick_data(data, stream=True)
                elif task_type == "bar":
                    self.database.save_bar_data(data, stream=True)

            except Empty:
                continue

            except Exception:
                self.active = False

                info: str = traceback.format_exc()
                self.write_log(f"触发异常，录制已停止：\n{info}")

    def close(self) -> None:
        """"""
        self.active = False

        if self.thread.is_alive():
            self.thread.join()

    def start(self) -> None:
        """"""
        self.active = True
        self.thread.start()

    def add_bar_recording(self, vt_symbol: str) -> None:
        """"""
        if vt_symbol in self.bar_recordings:
            self.write_log(f"已在K线记录列表中：{vt_symbol}")
            return

        if Exchange.LOCAL.value not in vt_symbol:
            contract: ContractData | None = self.main_engine.get_contract(vt_symbol)
            if not contract:
                self.write_log(f"找不到合约：{vt_symbol}")
                return

            self.bar_recordings[vt_symbol] = {
                "symbol": contract.symbol,
                "exchange": contract.exchange.value,
                "gateway_name": contract.gateway_name
            }

            self.subscribe(contract)
        else:
            self.bar_recordings[vt_symbol] = {}

        self.save_setting()
        self.put_event()

        self.write_log(f"添加K线记录成功：{vt_symbol}")

    def add_tick_recording(self, vt_symbol: str) -> None:
        """"""
        if vt_symbol in self.tick_recordings:
            self.write_log(f"已在Tick记录列表中：{vt_symbol}")
            return

        # For normal contract
        if Exchange.LOCAL.value not in vt_symbol:
            contract: ContractData | None = self.main_engine.get_contract(vt_symbol)
            if not contract:
                self.write_log(f"找不到合约：{vt_symbol}")
                return

            self.tick_recordings[vt_symbol] = {
                "symbol": contract.symbol,
                "exchange": contract.exchange.value,
                "gateway_name": contract.gateway_name
            }

            self.subscribe(contract)
        # No need to subscribe for spread data
        else:
            self.tick_recordings[vt_symbol] = {}

        self.save_setting()
        self.put_event()

        self.write_log(f"添加Tick记录成功：{vt_symbol}")

    def remove_bar_recording(self, vt_symbol: str) -> None:
        """"""
        if vt_symbol not in self.bar_recordings:
            self.write_log(f"不在K线记录列表中：{vt_symbol}")
            return

        self.bar_recordings.pop(vt_symbol)
        self.save_setting()
        self.put_event()

        self.write_log(f"移除K线记录成功：{vt_symbol}")

    def remove_tick_recording(self, vt_symbol: str) -> None:
        """"""
        if vt_symbol not in self.tick_recordings:
            self.write_log(f"不在Tick记录列表中：{vt_symbol}")
            return

        self.tick_recordings.pop(vt_symbol)
        self.save_setting()
        self.put_event()

        self.write_log(f"移除Tick记录成功：{vt_symbol}")

    def register_event(self) -> None:
        """"""
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
        self.event_engine.register(EVENT_TICK, self.process_tick_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_contract_event)
        self.event_engine.register(EVENT_SPREAD_DATA, self.process_spread_event)

    def update_tick(self, tick: TickData) -> None:
        """"""
        # 过滤偏离本地时间戳过大的Tick数据
        tick_delta: timedelta = abs(tick.datetime - self.filter_dt)
        if abs(tick_delta) >= self.filter_delta:
            return

        # 收到有效（新鲜）Tick，标记行情存活
        self.last_tick_dt = datetime.now(DB_TZ)

        if tick.vt_symbol in self.tick_recordings:
            self.record_tick(copy(tick))

        if tick.vt_symbol in self.bar_recordings:
            bg: BarGenerator = self.get_bar_generator(tick.vt_symbol)
            bg.update_tick(copy(tick))

    def _market_closed(self) -> bool:
        """休場モード判定：KBSゲートウェイの market_closed_mode。"""
        gateway = self.main_engine.get_gateway("KBS")
        return bool(getattr(gateway, "market_closed_mode", False))

    def process_timer_event(self, event: Event) -> None:
        """"""
        now = datetime.now()
        current_time = now.time()

        # Recording windows (ザラバ 8:45～15:40 / 17:00～翌5:55)
        #   Option daily : 8:46～15:35 and 17:01～翌1:55
        #   Option 15m   : 8:46～15:35 and 17:01～翌5:54 (wider — intraday only)
        #   Bar / Tick   : 8:46～15:39 and 17:01～翌5:59 (wider superset)
        in_option_window: bool = (
            time(8, 46) <= current_time <= time(15, 35)
            or current_time >= time(17, 1)
            or current_time <= time(1, 55)
        )
        in_option15m_window: bool = (
            time(8, 46) <= current_time <= time(15, 35)
            or current_time >= time(17, 1)
            or current_time <= time(5, 54)
        )
        in_bartick_window: bool = (
            time(8, 46) <= current_time <= time(15, 39)
            or current_time >= time(17, 1)
            or current_time <= time(5, 54)
        )

        self.filter_dt = datetime.now(DB_TZ)

        # Outside the (wider) bar/tick window → nothing is recorded.
        if not in_bartick_window:
            self.bars.clear()
            self.ticks.clear()
            return

        self.timer_count += 1
        self.option_timer_count += 1
        if self.timer_count < self.timer_interval:
            return
        self.timer_count = 0

        # 行情存活判定：休市日（假日）网关只在启动时推送一次陈旧的快照Tick，
        # 之后没有Tick。若在 feed_alive_window 秒内没有收到有效Tick，则视为
        # 行情未存活，不保存K线与期权数据（并清空缓存）。
        feed_alive: bool = (
            self.last_tick_dt is not None
            and (datetime.now(DB_TZ) - self.last_tick_dt).total_seconds()
            <= self.feed_alive_window
        )
        if not feed_alive:
            print(f"[__] 行情未存活，不保存K线与期权数据（并清空缓存）")
            self.bars.clear()
            self.ticks.clear()
            return

        # 休場モード（KBSゲートウェイ market_closed_mode）では、オプション
        # データと Tick データを保存しない。
        market_closed: bool = self._market_closed()
        if market_closed:
            print(f"[__] 休場モードでは、オプションデータと Tick データを保存しない。")
            self.bars.clear()
            self.ticks.clear()
            return

        # Record option data every minute.
        #   15m intraday bar  : written while in_option15m_window (…～翌5:54)
        #   DAILY snapshot    : written only while in_option_window (…～翌1:55)
        if (
            in_option15m_window
            and self.option_timer_count >= self.option_timer_interval
        ):
            self.record_all_option_data(write_daily=in_option_window)
            self.option_timer_count = 0

        for bars in self.bars.values():
            self.queue.put(("bar", bars))
        self.bars.clear()

        for ticks in self.ticks.values():
            self.queue.put(("tick", ticks))
        self.ticks.clear()

    def process_tick_event(self, event: Event) -> None:
        """"""
        tick: TickData = event.data
        self.update_tick(tick)

    def process_contract_event(self, event: Event) -> None:
        """"""
        contract: ContractData = event.data
        vt_symbol: str = contract.vt_symbol

        if (vt_symbol in self.tick_recordings or vt_symbol in self.bar_recordings):
            self.subscribe(contract)

    def process_spread_event(self, event: Event) -> None:
        """"""
        spread_item: SpreadItem = event.data
        tick: TickData = TickData(
            symbol=spread_item.name,
            exchange=Exchange.LOCAL,
            datetime=spread_item.datetime,
            name=spread_item.name,
            last_price=(spread_item.bid_price + spread_item.ask_price) / 2,
            bid_price_1=spread_item.bid_price,
            ask_price_1=spread_item.ask_price,
            bid_volume_1=spread_item.bid_volume,
            ask_volume_1=spread_item.ask_volume,
            localtime=spread_item.datetime,
            gateway_name="SPREAD"
        )

        # Filter not inited spread data
        if tick.datetime:
            self.update_tick(tick)

    def write_log(self, msg: str) -> None:
        """"""
        event: Event = Event(
            EVENT_RECORDER_LOG,
            msg
        )
        self.event_engine.put(event)

    def put_event(self) -> None:
        """"""
        tick_symbols: list[str] = list(self.tick_recordings.keys())
        tick_symbols.sort()

        bar_symbols: list[str] = list(self.bar_recordings.keys())
        bar_symbols.sort()

        data: dict = {
            "tick": tick_symbols,
            "bar": bar_symbols
        }

        event: Event = Event(
            EVENT_RECORDER_UPDATE,
            data
        )
        self.event_engine.put(event)

    def record_tick(self, tick: TickData) -> None:
        """"""
        self.ticks[tick.vt_symbol].append(tick)

    def record_bar(self, bar: BarData, new_minute: bool = True) -> None:
        """"""
        self.bars[bar.vt_symbol].append(bar)

    def get_bar_generator(self, vt_symbol: str) -> BarGenerator:
        """"""
        bg: BarGenerator | None = self.bar_generators.get(vt_symbol, None)

        if not bg:
            bg = BarGenerator(self.record_bar)
            bg.main_engine = self.main_engine
            self.bar_generators[vt_symbol] = bg

        return bg

    def subscribe(self, contract: ContractData) -> None:
        """"""
        req: SubscribeRequest = SubscribeRequest(
            symbol=contract.symbol,
            exchange=contract.exchange
        )
        self.main_engine.subscribe(req, contract.gateway_name)

    def _build_option_bar(
        self, contract: ContractData, instrument, dt: datetime, interval: Interval
    ) -> BarData:
        """Build one option snapshot bar (IV/greeks/strike) for the given interval."""
        last_price = getattr(instrument, "last_price", 0)
        bar = BarData(
            gateway_name=contract.gateway_name,
            symbol=contract.symbol,
            exchange=contract.exchange,
            datetime=dt,
            interval=interval,
            volume=getattr(instrument, "volume", 0),
            open_interest=getattr(instrument, "open_interest", 0),
            open_price=last_price,
            high_price=last_price,
            low_price=last_price,
            close_price=last_price,
            futures_option_type=2,  # Assuming 2 represents options
        )
        if instrument.tick:
            bar.n225_vi = instrument.tick.n225_vi
        bar.strike = getattr(instrument, "strike_price", 0)
        bar.iv = getattr(instrument, "mid_impv", 0)
        bar.delta = getattr(instrument, "theo_delta", 0)
        bar.gamma = getattr(instrument, "theo_gamma", 0)
        bar.vega = getattr(instrument, "theo_vega", 0)
        bar.theta = getattr(instrument, "theo_theta", 0)
        return bar

    def record_all_option_data(self, write_daily: bool = True) -> None:
        """
        Record data for all strike options from OptionMaster.

        Writes per contract:
          * DAILY   (datetime = session_end 15:45) — end-of-session snapshot.
                    Only when write_daily=True (option window …～翌1:55).
          * 15m     (datetime = current 15-min bucket) — intraday per-strike
                    IV, upserted each minute so the bucket holds the latest
                    snapshot. Written for the wider 15m window (…～翌5:54).
                    Used by the IV時系列 pinned (固定行使価格) line.
        """
        option_engine = self.main_engine.get_engine("OptionMaster")
        if not option_engine:
            self.write_log("OptionMaster engine not loaded, cannot record option data.")
            return

        all_contracts = self.main_engine.get_all_contracts()
        option_contracts = [c for c in all_contracts if c.product == Product.OPTION]

        if not option_contracts:
            self.write_log("No option contracts found.")
            return

        self.write_log(f"Starting to record data for {len(option_contracts)} option contracts.")

        now: datetime = datetime.now(DB_TZ)
        session_end: datetime = now.replace(hour=15, minute=45, second=0, microsecond=0)
        if now.hour >= 17:
            session_end = session_end + timedelta(days=1)
        # Current 15-minute bucket for intraday per-strike recording.
        bucket_15m: datetime = now.replace(
            minute=(now.minute // 15) * 15, second=0, microsecond=0
        )

        for contract in option_contracts:
            instrument = option_engine.get_instrument(contract.vt_symbol)
            if not instrument:
                continue

            if write_daily:
                self.record_bar(
                    self._build_option_bar(contract, instrument, session_end, Interval.DAILY)
                )
            self.record_bar(
                self._build_option_bar(contract, instrument, bucket_15m, Interval.MINUTE15)
            )

        kinds: str = "daily + 15m" if write_daily else "15m"
        self.write_log(f"Finished recording option data ({kinds}) for {len(option_contracts)} contracts.")
