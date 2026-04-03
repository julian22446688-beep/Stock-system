from __future__ import annotations
import argparse
import csv
import hashlib
import json
import math
import os
import re
import statistics
import tempfile
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
import requests
ALPACA_API_KEY = 'YOUR_ALPACA_API_KEY'
ALPACA_SECRET_KEY = 'YOUR_ALPACA_SECRET_KEY'
FMP_API_KEY = 'YOUR_FMP_API_KEY'
FINNHUB_API_KEY = 'YOUR_FINNHUB_API_KEY'
POLYGON_API_KEY = 'YOUR_POLYGON_OR_MASSIVE_API_KEY'
MASSIVE_API_KEY = POLYGON_API_KEY
FRED_API_KEY = 'YOUR_FRED_API_KEY'
SEC_USER_AGENT = 'Oscar Sanchez Julian22224444@gmail.com'
TICKER_LIST: list[str] = []
REQUEST_TIMEOUT = 30
HTTP_RETRIES = 3
HTTP_BACKOFF_SECONDS = 1.5
ALPACA_DATA_BASE = 'https://data.alpaca.markets'
FMP_BASE = 'https://financialmodelingprep.com'
FINNHUB_BASE = 'https://finnhub.io/api/v1'
POLYGON_BASE = 'https://api.polygon.io'
SEC_DATA_BASE = 'https://data.sec.gov'
SEC_ARCHIVES_BASE = 'https://www.sec.gov/Archives'
FRED_BASE = 'https://api.stlouisfed.org/fred'
OUTPUT_COLUMNS = ['ticker', 'list_source', 'company_name', 'exchange', 'sector', 'industry', 'gics_4digit', 'market_cap', 'market_cap_tier', 'adr_flag', 'chinese_adr_flag', 'dual_listing_flag', 'saas_high_growth_flag', 'financial_reit_utility_asset_light_flag', 'sector_classification_rule', 'altman_model_variant', 'solvency_hard_floor_rule_used', 'current_price', 'price_as_of', 'sma_50', 'sma_200', 'pct_vs_200sma', 'return_12_1_skip_month', 'hv30_annualized', 'adtv_20d_dollar', 'bid_ask_spread_pct', 'high_52w', 'high_52w_date', 'breakout_volume_ratio', 'up_day_volume_ratio_20d', 'golden_cross_flag', 'death_cross_flag', 'sector_etf_ticker', 'sector_etf_price', 'sector_etf_sma_200', 'sector_etf_above_200sma', 'sector_hv_median', 'float_shares', 'short_interest_pct_float', 'days_to_cover', 'position_to_adtv_pct', 'last_earnings_date', 'trading_days_since_earnings', 'ear_3d_abnormal_return', 'ear_revenue_beat', 'ear_eps_beat', 'analyst_count', 'revision_momentum_pct', 'revision_direction', 'ear_neutral_flag', 'ear_exempt_reason', 'roic', 'roic_source', 'wacc', 'roic_minus_wacc', 'roic_trend_2q', 'fcf_ni_ratio', 'sbc_pct_revenue', 'economic_roic_used', 'roe', 'nim_trend', 'fre_growth_pct', 'fcf_fy0', 'fcf_fy1', 'fcf_fy2', 'fcf_fy3', 'free_cash_flow_negative_3y_streak', 'net_income_ttm', 'ocf_ttm', 'revenue_ttm', 'total_assets_cur', 'total_assets_prior', 'sloan_ratio', 'ocf_ni_ratio', 'ocf_revenue_ratio', 'sector_sloan_threshold', 'sector_ocf_revenue_median', 'etr_change_qoq', 'ar_growth_vs_revenue', 'inventory_growth_vs_revenue', 'asset_growth_yoy', 'net_dilution_pct', 'persistent_ocf_ni_divergence_flag', 'revenue_vs_earnings_divergence_flag', 'peg_ratio', 'forward_pe', 'eps_growth_5y_consensus', 'ev_ebitda', 'ev_ebitda_sector_median', 'gpa_ratio', 'gpa_sector_median', 'cash', 'total_debt', 'net_cash_flag', 'net_debt_ebitda', 'price_to_sales', 'ps_sector_median', 'valuation_soft_flag', 'altman_z_score', 'piotroski_score', 'chs_result', 'altman_piotroski_conflict_flag', 'dtc_7_15_modifier_flag', 'dtc_independence_check', 'latest_insider_filing_date', 'latest_insider_name', 'latest_insider_title', 'latest_insider_transaction_code', 'latest_insider_shares', 'latest_insider_value', 'insider_holdings_pct', 'insider_holdings_verified', 'insider_fallback_threshold_used', 'sec_open_market_confirmed', 'sec_10b5_1_mentioned', 'sec_10b5_1_regime', 'sec_10b5_1_adoption_date', 'cluster_buy_flag', 'cluster_buy_count', 'cluster_buy_members', 'signal_age_days', 'si_pct_at_filing', 'buy_within_10d_earnings_flag', 'institutional_convergence_flag', 'options_flow_flag', 'si_trending_down_flag', 'ttm_insider_buy_vs_dilution', 'sector_underperformance_24m', 'ceo_cfo_drawdown_buy_flag', 'sec_cik', 'sec_recent_form4_count_240d', 'sec_last_form4_accession', 'sec_last_form4_url', 'sec_secondary_offering_flag', 'hard_floor_result', 'hard_floor_fail_reasons', 'stacking_condition_1_dtc', 'stacking_condition_2_si', 'stacking_condition_3_vol', 'stacking_condition_4_holdings', 'stacking_condition_5_signal_age', 'stacking_condition_6_eq', 'stacking_condition_7_conflict', 'stacking_condition_8_price', 'stacking_count', 'stacking_result', 'ccg_momentum_score', 'ccg_profitability_score', 'ccg_earnings_quality_score', 'ccg_ear_score', 'ccg_ear_exempt', 'ccg_weight_used', 'core_conviction_score', 'ccg_result', 'f1_momentum_raw', 'f1_momentum_adjusted', 'f1_weighted', 'f2_earnings_quality_raw', 'f2_earnings_quality_adjusted', 'f2_weighted', 'f3_profitability_raw', 'f3_profitability_adjusted', 'f3_weighted', 'f4_ear_raw', 'f4_ear_adjusted', 'f4_weighted', 'f5_insider_raw', 'f5_insider_adjusted', 'f5_weighted', 'f6_valuation_raw', 'f6_valuation_adjusted', 'f6_weighted', 'f7_solvency_raw', 'f7_solvency_adjusted', 'f7_weighted', 'raw_composite', 'standalone_deduction_vol', 'standalone_deduction_si_dtc', 'standalone_deduction_eq', 'standalone_deduction_concentration', 'standalone_deduction_rates', 'total_standalone_deductions', 'regime_deduction_soft_crash', 'regime_deduction_full_crash', 'regime_deduction_mtum_vtv', 'regime_deduction_hy_oas', 'total_regime_deductions', 'final_composite', 'momentum_dependent_entry_flag', 'zeroed_momentum_composite', 'override_1_holdings', 'override_2_mtum_vtv', 'override_3_soft_crash_insider', 'override_4a_full_crash', 'override_4b_soft_crash_price', 'override_5_below_200sma', 'mid_tier_cap_applied', 'small_tier_cap_applied', 'final_tier', 'tier_floor_pct', 'tier_range_pct', 'composite_minus_floor', 'tier_score_range', 'composite_proportional_size', 'atr_20d', 'atr_stop_implied_pct', 'atr_adjusted_size', 'final_position_size_pct', 'manual_review_chs_required', 'manual_review_path_to_positive_fcf', 'manual_review_ceo_cfo_self_sale', 'manual_review_secondary_offering_after_buy', 'manual_review_correlation_to_top3', 'manual_review_chinese_adr_dual_listing', 'manual_review_dtc_independence', 'manual_review_holdings_verified', 'manual_review_sector_etf_representative', 'manual_review_portfolio_context', 'manual_review_regime_state', 'manual_review_notes', 'script_version', 'run_id', 'run_mode', 'source_market_data', 'source_quote', 'source_profile', 'source_cashflow', 'source_scores', 'source_analyst', 'source_insider', 'source_sec', 'null_fields']

def _http_get_json(url: str, params: Optional[dict[str, Any]]=None, headers: Optional[dict[str, str]]=None) -> Any:
    last_error = None
    for attempt in range(HTTP_RETRIES):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.status_code >= 400:
                raise RuntimeError(f'HTTP {resp.status_code} for {url}: {resp.text[:200]}')
            return resp.json()
        except Exception as e:
            last_error = e
            if attempt < HTTP_RETRIES - 1:
                time.sleep(HTTP_BACKOFF_SECONDS * (attempt + 1))
    raise last_error if last_error else RuntimeError(f'HTTP failure for {url}')

def _http_get_text(url: str, params: Optional[dict[str, Any]]=None, headers: Optional[dict[str, str]]=None) -> str:
    last_error = None
    for attempt in range(HTTP_RETRIES):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            if resp.status_code >= 400:
                raise RuntimeError(f'HTTP {resp.status_code} for {url}: {resp.text[:200]}')
            return resp.text
        except Exception as e:
            last_error = e
            if attempt < HTTP_RETRIES - 1:
                time.sleep(HTTP_BACKOFF_SECONDS * (attempt + 1))
    raise last_error if last_error else RuntimeError(f'HTTP failure for {url}')

def _alpaca_headers() -> dict[str, str]:
    return {'APCA-API-KEY-ID': ALPACA_API_KEY, 'APCA-API-SECRET-KEY': ALPACA_SECRET_KEY}

def _sec_headers() -> dict[str, str]:
    return {'User-Agent': SEC_USER_AGENT, 'Accept-Encoding': 'gzip, deflate', 'Accept': 'application/json, text/plain, */*'}

def alpaca_get_stock_bars(symbols: list[str], start: str, end: str, timeframe: str='1Day', limit: int=10000, feed: str='iex') -> dict[str, Any]:
    url = f'{ALPACA_DATA_BASE}/v2/stocks/bars'
    params = {'symbols': ','.join(symbols), 'timeframe': timeframe, 'start': start, 'end': end, 'limit': limit, 'feed': feed, 'adjustment': 'raw'}
    combined = {'bars': {}}
    page_token = None
    while True:
        if page_token:
            params['page_token'] = page_token
        payload = _http_get_json(url, params=params, headers=_alpaca_headers())
        bars = payload.get('bars', {})
        if isinstance(bars, dict):
            for sym, sym_bars in bars.items():
                combined['bars'].setdefault(sym, [])
                combined['bars'][sym].extend(sym_bars if isinstance(sym_bars, list) else [])
        page_token = payload.get('next_page_token')
        if not page_token:
            break
    return combined

def alpaca_get_latest_bars(symbols: list[str], feed: str='iex') -> dict[str, Any]:
    return _http_get_json(f'{ALPACA_DATA_BASE}/v2/stocks/bars/latest', params={'symbols': ','.join(symbols), 'feed': feed}, headers=_alpaca_headers())

def alpaca_get_latest_quotes(symbols: list[str], feed: str='iex') -> dict[str, Any]:
    return _http_get_json(f'{ALPACA_DATA_BASE}/v2/stocks/quotes/latest', params={'symbols': ','.join(symbols), 'feed': feed}, headers=_alpaca_headers())

def massive_get_aggregates(ticker: str, date_from: str, date_to: str, adjusted: bool=True) -> dict[str, Any]:
    return _http_get_json(f'{POLYGON_BASE}/v2/aggs/ticker/{ticker}/range/1/day/{date_from}/{date_to}', params={'adjusted': 'true' if adjusted else 'false', 'sort': 'asc', 'limit': 50000, 'apiKey': MASSIVE_API_KEY})

def massive_get_snapshot(ticker: str) -> dict[str, Any]:
    return _http_get_json(f'{POLYGON_BASE}/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}', params={'apiKey': MASSIVE_API_KEY})

def massive_get_last_trade(ticker: str) -> dict[str, Any]:
    return _http_get_json(f'{POLYGON_BASE}/v2/last/trade/{ticker}', params={'apiKey': MASSIVE_API_KEY})

def massive_get_last_quote(ticker: str) -> dict[str, Any]:
    return _http_get_json(f'{POLYGON_BASE}/v2/last/nbbo/{ticker}', params={'apiKey': MASSIVE_API_KEY})

def massive_get_ticker_details(ticker: str) -> dict[str, Any]:
    return _http_get_json(f'{POLYGON_BASE}/v3/reference/tickers/{ticker}', params={'apiKey': MASSIVE_API_KEY})

def massive_get_short_interest(ticker: str) -> dict[str, Any]:
    return _http_get_json(f'{POLYGON_BASE}/v3/shorts/short-interest/{ticker}', params={'apiKey': MASSIVE_API_KEY})

def fmp_get_profile(ticker: str) -> Any:
    return _http_get_json(f'{FMP_BASE}/api/v3/profile/{ticker}', params={'apikey': FMP_API_KEY})

def fmp_get_company_core_information(ticker: str) -> Any:
    return _http_get_json(f'{FMP_BASE}/api/v4/company-core-information', params={'symbol': ticker, 'apikey': FMP_API_KEY})

def fmp_get_key_metrics_ttm(ticker: str) -> Any:
    return _http_get_json(f'{FMP_BASE}/api/v3/key-metrics-ttm/{ticker}', params={'apikey': FMP_API_KEY})

def fmp_get_ratios_ttm(ticker: str) -> Any:
    return _http_get_json(f'{FMP_BASE}/api/v3/ratios-ttm/{ticker}', params={'apikey': FMP_API_KEY})

def fmp_get_key_metrics_quarter(ticker: str, limit: int=8) -> Any:
    return _http_get_json(f'{FMP_BASE}/api/v3/key-metrics/{ticker}', params={'period': 'quarter', 'limit': limit, 'apikey': FMP_API_KEY})

def fmp_get_ratios_quarter(ticker: str, limit: int=8) -> Any:
    return _http_get_json(f'{FMP_BASE}/api/v3/ratios/{ticker}', params={'period': 'quarter', 'limit': limit, 'apikey': FMP_API_KEY})

def fmp_get_dcf(ticker: str) -> Any:
    return _http_get_json(f'{FMP_BASE}/api/v3/discounted-cash-flow/{ticker}', params={'apikey': FMP_API_KEY})

def fmp_get_cash_flow_annual(ticker: str, limit: int=4) -> Any:
    return _http_get_json(f'{FMP_BASE}/api/v3/cash-flow-statement/{ticker}', params={'period': 'annual', 'limit': limit, 'apikey': FMP_API_KEY})

def fmp_get_income_statement_quarter(ticker: str, limit: int=8) -> Any:
    return _http_get_json(f'{FMP_BASE}/api/v3/income-statement/{ticker}', params={'period': 'quarter', 'limit': limit, 'apikey': FMP_API_KEY})

def fmp_get_balance_sheet_annual(ticker: str, limit: int=4) -> Any:
    return _http_get_json(f'{FMP_BASE}/api/v3/balance-sheet-statement/{ticker}', params={'period': 'annual', 'limit': limit, 'apikey': FMP_API_KEY})

def fmp_get_balance_sheet_quarter(ticker: str, limit: int=8) -> Any:
    return _http_get_json(f'{FMP_BASE}/api/v3/balance-sheet-statement/{ticker}', params={'period': 'quarter', 'limit': limit, 'apikey': FMP_API_KEY})

def fmp_get_enterprise_values(ticker: str, limit: int=4) -> Any:
    return _http_get_json(f'{FMP_BASE}/api/v3/enterprise-values/{ticker}', params={'limit': limit, 'apikey': FMP_API_KEY})

def fmp_get_analyst_estimates(ticker: str) -> Any:
    return _http_get_json(f'{FMP_BASE}/api/v3/analyst-estimates/{ticker}', params={'apikey': FMP_API_KEY})

def fmp_get_financial_scores(ticker: str) -> Any:
    return _http_get_json(f'{FMP_BASE}/api/v4/score', params={'symbol': ticker, 'apikey': FMP_API_KEY})

def fmp_get_earnings_surprises(ticker: str) -> Any:
    return _http_get_json(f'{FMP_BASE}/api/v3/earnings-surprises/{ticker}', params={'apikey': FMP_API_KEY})

def fmp_get_earnings_report(ticker: str) -> Any:
    return _http_get_json(f'{FMP_BASE}/stable/earnings', params={'symbol': ticker, 'apikey': FMP_API_KEY})

def finnhub_get_metrics(ticker: str) -> Any:
    return _http_get_json(f'{FINNHUB_BASE}/stock/metric', params={'symbol': ticker, 'metric': 'all', 'token': FINNHUB_API_KEY})

def finnhub_get_earnings_surprises(ticker: str) -> Any:
    return _http_get_json(f'{FINNHUB_BASE}/stock/earnings', params={'symbol': ticker, 'token': FINNHUB_API_KEY})

def finnhub_get_earnings_calendar(ticker: str, from_date: str, to_date: str) -> Any:
    return _http_get_json(f'{FINNHUB_BASE}/calendar/earnings', params={'symbol': ticker, 'from': from_date, 'to': to_date, 'token': FINNHUB_API_KEY})

def finnhub_get_recommendations(ticker: str) -> Any:
    return _http_get_json(f'{FINNHUB_BASE}/stock/recommendation', params={'symbol': ticker, 'token': FINNHUB_API_KEY})

def finnhub_get_insider_transactions(ticker: str, from_date: str, to_date: str) -> Any:
    return _http_get_json(f'{FINNHUB_BASE}/stock/insider-transactions', params={'symbol': ticker, 'from': from_date, 'to': to_date, 'token': FINNHUB_API_KEY})

def sec_get_company_submissions(cik: str) -> Any:
    return _http_get_json(f'{SEC_DATA_BASE}/submissions/CIK{str(cik).zfill(10)}.json', headers=_sec_headers())

def sec_get_companyfacts(cik: str) -> Any:
    return _http_get_json(f'{SEC_DATA_BASE}/api/xbrl/companyfacts/CIK{str(cik).zfill(10)}.json', headers=_sec_headers())

def sec_get_filing_text(url_or_path: str) -> str:
    url = url_or_path if url_or_path.startswith('http') else f"{SEC_ARCHIVES_BASE}/{url_or_path.lstrip('/')}"
    return _http_get_text(url, headers=_sec_headers())

def fred_get_series_observations(series_id: str) -> Any:
    return _http_get_json(f'{FRED_BASE}/series/observations', params={'series_id': series_id, 'api_key': FRED_API_KEY, 'file_type': 'json'})

def _none_dict(fields: list[str]) -> dict[str, Any]:
    return {f: None for f in fields}

def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == '':
            return None
        return float(value)
    except Exception:
        return None

def _to_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == '':
            return None
        return int(float(value))
    except Exception:
        return None

def _safe_div(n: Optional[float], d: Optional[float]) -> Optional[float]:
    if n is None or d is None or d == 0:
        return None
    return n / d

def _pct_change(new: Optional[float], old: Optional[float]) -> Optional[float]:
    if new is None or old is None or old == 0:
        return None
    return (new / old - 1.0) * 100.0

def _first_list_item(payload: Any) -> Any:
    if isinstance(payload, list) and payload:
        return payload[0]
    return payload if isinstance(payload, dict) else {}

def _get_nested(obj: Any, *path: str) -> Any:
    cur = obj
    for p in path:
        if isinstance(cur, dict):
            cur = cur.get(p)
        else:
            return None
    return cur

def _parse_iso_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        v = value.strip()
        if not v:
            return None
        for fmt in ('%Y-%m-%d', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S.%fZ'):
            try:
                return datetime.strptime(v, fmt).date().isoformat()
            except Exception:
                pass
        try:
            return datetime.fromisoformat(v.replace('Z', '+00:00')).date().isoformat()
        except Exception:
            return None
    if isinstance(value, (int, float)):
        try:
            return datetime.utcfromtimestamp(float(value) / 1000.0).date().isoformat()
        except Exception:
            return None
    return None

def _latest_non_null(values: list[Any]) -> Any:
    for v in values:
        if v is not None and v != '':
            return v
    return None

def _bars_from_alpaca(raw_bars: Any, symbol: str) -> list[dict[str, Any]]:
    bars = _get_nested(raw_bars, 'bars', symbol)
    return bars if isinstance(bars, list) else []

def _bars_from_massive(raw_bars: Any) -> list[dict[str, Any]]:
    results = _get_nested(raw_bars, 'results')
    return results if isinstance(results, list) else []

def _extract_close_from_bar(bar: dict[str, Any]) -> Optional[float]:
    return _to_float(_latest_non_null([bar.get('c'), bar.get('close')]))

def _extract_high_from_bar(bar: dict[str, Any]) -> Optional[float]:
    return _to_float(_latest_non_null([bar.get('h'), bar.get('high')]))

def _extract_low_from_bar(bar: dict[str, Any]) -> Optional[float]:
    return _to_float(_latest_non_null([bar.get('l'), bar.get('low')]))


def _extract_volume_from_bar(bar: dict[str, Any]) -> Optional[float]:
    return _to_float(_latest_non_null([bar.get('v'), bar.get('volume')]))

def _extract_bar_date(bar: dict[str, Any]) -> Optional[str]:
    return _parse_iso_date(_latest_non_null([bar.get('t'), bar.get('timestamp')]))

def _sort_bars_chronologically(bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    valid = []
    for b in bars or []:
        d = _extract_bar_date(b)
        if d is None:
            continue
        valid.append((d, b))
    valid.sort(key=lambda x: x[0])
    return [b for _, b in valid]

def _compute_sma(closes: list[Optional[float]], window: int) -> Optional[float]:
    vals = [c for c in closes if c is not None]
    if len(vals) < window:
        return None
    return sum(vals[-window:]) / float(window)

def _compute_hv30_from_closes(closes: list[Optional[float]]) -> Optional[float]:
    vals = [c for c in closes if c is not None]
    if len(vals) < 31:
        return None
    rets = []
    for i in range(-30, 0):
        c1 = vals[i - 1]
        c2 = vals[i]
        if c1 <= 0 or c2 <= 0:
            return None
        rets.append(math.log(c2 / c1))
    return statistics.stdev(rets) * math.sqrt(252.0) * 100.0

def _compute_adtv20(closes: list[Optional[float]], volumes: list[Optional[float]]) -> Optional[float]:
    pairs = [(c, v) for c, v in zip(closes[-20:], volumes[-20:]) if c is not None and v is not None]
    if len(pairs) < 20:
        return None
    return sum((c * v for c, v in pairs)) / 20.0


def _compute_breakout_volume_ratio(volumes: list[Optional[float]]) -> Optional[float]:
    vals = [v for v in volumes if v is not None]
    if len(vals) < 21:
        return None
    return _safe_div(vals[-1], sum(vals[-21:-1]) / 20.0)

def _compute_up_down_volume_ratio_20d(closes: list[Optional[float]], volumes: list[Optional[float]]) -> Optional[float]:
    if len(closes) < 21 or len(volumes) < 21:
        return None
    up_vol = 0.0
    down_vol = 0.0
    valid = 0
    for i in range(len(closes) - 20, len(closes)):
        c = closes[i]
        prev_c = closes[i - 1]
        v = volumes[i]
        if c is None or prev_c is None or v is None:
            continue
        valid += 1
        if c > prev_c:
            up_vol += v
        elif c < prev_c:
            down_vol += v
    if valid < 20 or down_vol == 0:
        return None
    return up_vol / down_vol

def _sector_etf_for_sector(sector: Optional[str]) -> Optional[str]:
    s = (sector or '').lower()
    if 'technology' in s:
        return 'XLK'
    if 'health' in s:
        return 'XLV'
    if 'financial' in s:
        return 'XLF'
    if 'consumer cyclical' in s or 'consumer discretionary' in s:
        return 'XLY'
    if 'consumer defensive' in s or 'consumer staples' in s:
        return 'XLP'
    if 'energy' in s:
        return 'XLE'
    if 'industrial' in s:
        return 'XLI'
    if 'basic materials' in s or 'materials' in s:
        return 'XLB'
    if 'real estate' in s:
        return 'XLRE'
    if 'utilities' in s:
        return 'XLU'
    if 'communication' in s:
        return 'XLC'
    return None

def _compute_true_range(cur_high: float, cur_low: float, prev_close: float) -> float:
    return max(cur_high - cur_low, abs(cur_high - prev_close), abs(cur_low - prev_close))

def _compute_atr20(highs: list[Optional[float]], lows: list[Optional[float]], closes: list[Optional[float]]) -> Optional[float]:
    vals = [(h, l, c) for h, l, c in zip(highs, lows, closes) if h is not None and l is not None and (c is not None)]
    if len(vals) < 21:
        return None
    trs = []
    for i in range(1, len(vals)):
        h, l, _ = vals[i]
        _, _, prev_c = vals[i - 1]
        trs.append(_compute_true_range(h, l, prev_c))
    if len(trs) < 20:
        return None
    return sum(trs[-20:]) / 20.0

def _extract_text_number(text: str, pattern: str) -> Optional[str]:
    m = re.search(pattern, text, flags=re.IGNORECASE)
    return m.group(1) if m else None

def _approx_trading_days_between(start_date_str: Optional[str], end_date: Optional[date]=None) -> Optional[int]:
    if start_date_str is None:
        return None
    try:
        d0 = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        d1 = end_date or date.today()
        return int(round(max((d1 - d0).days, 0) * 5.0 / 7.0))
    except Exception:
        return None

def parse_liquidity_and_short_section(raw_massive_short_interest: Any, raw_finnhub_metrics: Any, adtv_20d_dollar: Optional[float], intended_position_dollar: Optional[float]=None) -> dict[str, Any]:
    fields = ['float_shares', 'short_interest_pct_float', 'days_to_cover', 'position_to_adtv_pct']
    out = _none_dict(fields)
    massive_result = _get_nested(raw_massive_short_interest, 'results')
    if isinstance(massive_result, dict):
        out['float_shares'] = _to_float(_latest_non_null([massive_result.get('float'), massive_result.get('float_shares')]))
        out['short_interest_pct_float'] = _to_float(_latest_non_null([massive_result.get('short_interest_percent'), massive_result.get('short_interest_pct_float'), massive_result.get('percent_of_float')]))
        out['days_to_cover'] = _to_float(_latest_non_null([massive_result.get('days_to_cover'), massive_result.get('dtc')]))
    metric = _get_nested(raw_finnhub_metrics, 'metric')
    if isinstance(metric, dict):
        if out['float_shares'] is None:
            out['float_shares'] = _to_float(_latest_non_null([metric.get('shareFloat'), metric.get('sharesFloat')]))
        if out['short_interest_pct_float'] is None:
            # shortInterestRatio is shares_short / avg_daily_volume = DTC, not SI% of float
            out['short_interest_pct_float'] = _to_float(_latest_non_null([metric.get('shortInterestPercent'), metric.get('shortPercentFloat'), metric.get('shortPercent')]))
        if out['days_to_cover'] is None:
            out['days_to_cover'] = _to_float(_latest_non_null([metric.get('daysToCover'), metric.get('shortInterestDaysToCover'), metric.get('shortInterestRatio')]))
    # position_to_adtv_pct is computed downstream where intended position sizing is actually known.
    out['position_to_adtv_pct'] = None
    return out

def parse_earnings_and_ear_section(raw_finnhub_earnings_surprises: Any, raw_finnhub_earnings_calendar: Any, raw_fmp_analyst_estimates: Any, raw_fmp_earnings_surprises: Any, raw_fmp_earnings_report: Any, raw_revision_source: Any, ticker_3d_post_earnings_return: Optional[float], sector_etf_3d_post_earnings_return: Optional[float], market_cap_tier: Optional[str]) -> dict[str, Any]:
    fields = ['last_earnings_date', 'trading_days_since_earnings', 'ear_3d_abnormal_return', 'ear_revenue_beat', 'ear_eps_beat', 'analyst_count', 'revision_momentum_pct', 'revision_direction', 'ear_neutral_flag', 'ear_exempt_reason']
    out = _none_dict(fields)
    finnhub_earn = raw_finnhub_earnings_surprises if isinstance(raw_finnhub_earnings_surprises, list) else []
    fmp_earn = raw_fmp_earnings_surprises if isinstance(raw_fmp_earnings_surprises, list) else []
    fmp_report = raw_fmp_earnings_report if isinstance(raw_fmp_earnings_report, list) else []

    def _earn_row_date(r: dict) -> Optional[str]:
        return _parse_iso_date(_latest_non_null([
            r.get('date'),
            r.get('calendarDate'),
            r.get('reportedDate'),
        ]))

    def _earn_row_sort_key(r: dict) -> str:
        return str(_earn_row_date(r) or '0000-00-00')

    combined_earn = []
    if finnhub_earn:
        combined_earn.extend(finnhub_earn)
    if fmp_earn:
        combined_earn.extend(fmp_earn)
    if fmp_report:
        combined_earn.extend(fmp_report)
    if combined_earn:
        _today_str = date.today().isoformat()
        combined_earn = [r for r in combined_earn if (_earn_row_date(r) or '9999-12-31') <= _today_str]
        combined_earn = sorted(combined_earn, key=_earn_row_sort_key, reverse=True)
    latest = combined_earn[0] if combined_earn else None
    if isinstance(latest, dict):
        out['last_earnings_date'] = _earn_row_date(latest)
        actual_eps = _to_float(_latest_non_null([latest.get('actual'), latest.get('epsActual'), latest.get('actualEarningResult')]))
        estimate_eps = _to_float(_latest_non_null([latest.get('estimate'), latest.get('epsEstimated'), latest.get('estimatedEarning')]))
        actual_rev = _to_float(_latest_non_null([latest.get('revenueActual'), latest.get('revenue'), latest.get('actualRevenue')]))
        estimate_rev = _to_float(_latest_non_null([latest.get('revenueEstimate'), latest.get('revenueEstimated'), latest.get('estimatedRevenue')]))
        out['ear_eps_beat'] = None if actual_eps is None or estimate_eps is None else actual_eps > estimate_eps
        out['ear_revenue_beat'] = None if actual_rev is None or estimate_rev is None else actual_rev > estimate_rev
    if out['last_earnings_date'] is None and isinstance(raw_finnhub_earnings_calendar, dict):
        earnings_list = raw_finnhub_earnings_calendar.get('earningsCalendar') or raw_finnhub_earnings_calendar.get('earningsCalendarList')
        if isinstance(earnings_list, list):
            _today = date.today()
            _past_dates = []
            for _item in earnings_list:
                _d = _parse_iso_date(_item.get('date'))
                if _d is not None:
                    try:
                        _dd = datetime.strptime(_d, '%Y-%m-%d').date()
                        if _dd <= _today:
                            _past_dates.append(_d)
                    except Exception:
                        pass
            out['last_earnings_date'] = max(_past_dates) if _past_dates else None
    out['trading_days_since_earnings'] = _approx_trading_days_between(out['last_earnings_date'])
    if ticker_3d_post_earnings_return is not None and sector_etf_3d_post_earnings_return is not None:
        out['ear_3d_abnormal_return'] = (ticker_3d_post_earnings_return - sector_etf_3d_post_earnings_return) * 100.0
    est_rows = raw_fmp_analyst_estimates if isinstance(raw_fmp_analyst_estimates, list) else []
    if est_rows:
        def _est_count_row_sort_key(r: dict) -> str:
            return str(r.get('date') or r.get('fiscalDateEnding') or '9999-12-31')
        _today_str = date.today().isoformat()
        future_est_rows = [r for r in est_rows if (r.get('date') or r.get('fiscalDateEnding') or '') >= _today_str]
        future_est_rows = sorted(future_est_rows, key=_est_count_row_sort_key)
        latest_est = future_est_rows[0] if future_est_rows else {}
        out['analyst_count'] = _to_int(_latest_non_null([
            latest_est.get('numberAnalystsEstimatedRevenue'),
            latest_est.get('numberAnalystEstimatedRevenue'),
            latest_est.get('numberAnalystsEstimatedEps'),
            latest_est.get('numberAnalystEstimatedEps'),
        ]))
    if isinstance(raw_revision_source, dict):
        up_revisions = _to_int(raw_revision_source.get('up_revisions')) or 0
        down_revisions = _to_int(raw_revision_source.get('down_revisions')) or 0
        total_revisions = _to_int(raw_revision_source.get('total_revisions'))
        if total_revisions is None:
            total_revisions = up_revisions + down_revisions
        if total_revisions < 2:
            out['revision_momentum_pct'] = None
            out['revision_direction'] = 'skipped_insufficient_analysts'
        else:
            out['revision_momentum_pct'] = (up_revisions - down_revisions) / max(total_revisions, 1) * 100.0
            out['revision_direction'] = 'up' if out['revision_momentum_pct'] >= 5 else 'down' if out['revision_momentum_pct'] <= -5 else 'flat_mixed'
    elif out['analyst_count'] is not None and out['analyst_count'] < 2:
        out['revision_momentum_pct'] = None
        out['revision_direction'] = 'skipped_insufficient_analysts'
    else:
        out['revision_momentum_pct'] = None
        out['revision_direction'] = 'skipped_no_revision_history_source'
    if market_cap_tier == 'Mid':
        insufficient_coverage = out['analyst_count'] is None or out['analyst_count'] < 2
    elif market_cap_tier == 'Small':
        insufficient_coverage = out['analyst_count'] is None or out['analyst_count'] < 1
    else:
        insufficient_coverage = False
    expired = out['trading_days_since_earnings'] is not None and out['trading_days_since_earnings'] > 63
    out['ear_neutral_flag'] = bool(expired or insufficient_coverage)
    out['ear_exempt_reason'] = 'expired_gt_63d' if expired else 'insufficient_analyst_coverage' if insufficient_coverage else None
    return out

def parse_valuation_section(raw_fmp_ratios_ttm: Any, raw_fmp_key_metrics_ttm: Any, raw_fmp_enterprise_values: Any, raw_fmp_balance_annual: Any, raw_fmp_income_ttm: Any, raw_fmp_analyst_estimates: Any, price_to_sales_fallback: Optional[float]=None, current_price: Optional[float]=None, raw_fmp_balance_quarter: Any=None) -> dict[str, Any]:
    fields = ['peg_ratio', 'forward_pe', 'eps_growth_5y_consensus', 'ev_ebitda', 'ev_ebitda_sector_median', 'gpa_ratio', 'gpa_sector_median', 'cash', 'total_debt', 'net_cash_flag', 'net_debt_ebitda', 'price_to_sales', 'ps_sector_median', 'valuation_soft_flag', '_requires_gpa_peer_benchmark']
    out = _none_dict(fields)
    ratios_ttm = _first_list_item(raw_fmp_ratios_ttm)
    km_ttm = _first_list_item(raw_fmp_key_metrics_ttm)
    ev_rows = raw_fmp_enterprise_values if isinstance(raw_fmp_enterprise_values, list) else []
    bal_a = raw_fmp_balance_annual if isinstance(raw_fmp_balance_annual, list) else []
    bal_q = raw_fmp_balance_quarter if isinstance(raw_fmp_balance_quarter, list) else []
    income_rows = raw_fmp_income_ttm if isinstance(raw_fmp_income_ttm, list) else []
    est_rows = raw_fmp_analyst_estimates if isinstance(raw_fmp_analyst_estimates, list) else []

    def _stmt_row_sort_key(r: dict) -> str:
        return str(_latest_non_null([
            r.get('date'),
            r.get('fillingDate'),
            r.get('acceptedDate'),
            r.get('calendarYear'),
            r.get('period'),
        ]) or '0000-00-00')

    ev_rows = sorted(ev_rows, key=_stmt_row_sort_key, reverse=True)
    bal_a = sorted(bal_a, key=_stmt_row_sort_key, reverse=True)
    bal_q = sorted(bal_q, key=_stmt_row_sort_key, reverse=True)
    income_rows = sorted(income_rows, key=_stmt_row_sort_key, reverse=True)
    _forward_pe_direct = _latest_non_null([_to_float(ratios_ttm.get('forwardPE')), _to_float(km_ttm.get('forwardPE'))])
    def _future_row_sort_key(r: dict) -> str:
        return str(r.get('date') or r.get('fiscalDateEnding') or '9999-12-31')
    _ntm_eps = None
    if est_rows:
        _today_str = date.today().isoformat()
        _future = [r for r in est_rows if (r.get('date') or r.get('fiscalDateEnding') or '') > _today_str]
        _future = sorted(_future, key=_future_row_sort_key)
        if _future:
            _ntm_eps = _to_float(_future[0].get('estimatedEpsAvg'))
    if _forward_pe_direct is not None:
        out['forward_pe'] = _forward_pe_direct
    elif current_price not in (None, 0) and _ntm_eps not in (None, 0):
        out['forward_pe'] = current_price / _ntm_eps
    else:
        out['forward_pe'] = None
    _growth_row = None
    if est_rows:
        def _est_row_sort_key(r: dict) -> str:
            return str(r.get('date') or r.get('fiscalDateEnding') or '9999-12-31')
        _today_str = date.today().isoformat()
        _future_growth_rows = [r for r in est_rows if (r.get('date') or r.get('fiscalDateEnding') or '') > _today_str]
        if _future_growth_rows:
            _future_growth_rows = sorted(_future_growth_rows, key=_est_row_sort_key)
            _growth_row = _future_growth_rows[0]

    _ticker_sym = str(_latest_non_null([km_ttm.get('symbol'), ratios_ttm.get('symbol')]) or '').strip().upper() or None
    out['eps_growth_5y_consensus'] = _latest_non_null([
        _to_float(MANUAL_FINVIZ_EPS_GROWTH_5Y_BY_TICKER.get(_ticker_sym)) if _ticker_sym else None,
        _to_float(_growth_row.get('estimatedEpsAvgGrowth5Y')) if _growth_row else None,
        _to_float(_growth_row.get('epsGrowth5Y')) if _growth_row else None,
    ])
    out['peg_ratio'] = _to_float(MANUAL_FINVIZ_PEG_RATIO_BY_TICKER.get(_ticker_sym)) if _ticker_sym else None
    if out['peg_ratio'] is None and out['forward_pe'] is not None and (out['eps_growth_5y_consensus'] not in (None, 0)):
        out['peg_ratio'] = out['forward_pe'] / out['eps_growth_5y_consensus']
    ev_row = ev_rows[0] if ev_rows else {}
    out['ev_ebitda'] = _latest_non_null([_to_float(km_ttm.get('enterpriseValueOverEBITDA')), _to_float(ev_row.get('evToEbitda')), _to_float(ev_row.get('enterpriseValueOverEBITDA'))])
    balance_source = bal_q if bal_q else bal_a
    if balance_source:
        out['cash'] = _latest_non_null([_to_float(balance_source[0].get('cashAndCashEquivalents')), _to_float(balance_source[0].get('cashAndShortTermInvestments'))])
        out['total_debt'] = _to_float(balance_source[0].get('totalDebt'))
    out['net_cash_flag'] = None if out['cash'] is None or out['total_debt'] is None else out['cash'] > out['total_debt']
    ebitda_ttm_fallback = _sum_latest_quarters(income_rows, 'ebitda', 4)
    ebitda = _latest_non_null([_to_float(ev_row.get('ebitda')), ebitda_ttm_fallback])
    if out['cash'] is not None and out['total_debt'] is not None and ebitda not in (None, 0):
        out['net_debt_ebitda'] = (out['total_debt'] - out['cash']) / ebitda
    else:
        out['net_debt_ebitda'] = None
    out['price_to_sales'] = _latest_non_null([_to_float(ratios_ttm.get('priceToSalesRatio')), _to_float(km_ttm.get('priceToSalesRatio')), price_to_sales_fallback])
    gross_profit_ttm = _sum_latest_quarters(income_rows, 'grossProfit', 4)
    total_assets_cur = _to_float(balance_source[0].get('totalAssets')) if len(balance_source) >= 1 else None
    if gross_profit_ttm is not None and total_assets_cur not in (None, 0):
        out['gpa_ratio'] = gross_profit_ttm / total_assets_cur
    else:
        out['gpa_ratio'] = None
    gross_margin = _latest_non_null([
        _to_float(ratios_ttm.get('grossProfitMargin')),
        _to_float(km_ttm.get('grossProfitMargin')),
    ])
    if gross_margin is None and gross_profit_ttm is not None and revenue_ttm not in (None, 0):
        gross_margin = gross_profit_ttm / revenue_ttm
    if gross_margin is not None and abs(float(gross_margin)) > 1.0:
        gross_margin = float(gross_margin) / 100.0
    out['_requires_gpa_peer_benchmark'] = None if gross_margin is None else float(gross_margin) < 0.35
    out['valuation_soft_flag'] = None
    return out

def parse_sec_filing_section(sec_cik: Optional[str], raw_sec_submissions: Any, latest_insider_filing_date: Optional[str]) -> dict[str, Any]:
    fields = ['sec_cik', 'sec_recent_form4_count_240d', 'sec_last_form4_accession', 'sec_last_form4_url', 'sec_secondary_offering_flag']
    out = _none_dict(fields)
    out['sec_cik'] = sec_cik
    recent = _get_nested(raw_sec_submissions, 'filings', 'recent') or {}
    forms = recent.get('form') if isinstance(recent, dict) else None
    accessions = recent.get('accessionNumber') if isinstance(recent, dict) else None
    primary_docs = recent.get('primaryDocument') if isinstance(recent, dict) else None
    filing_dates = recent.get('filingDate') if isinstance(recent, dict) else None
    if isinstance(forms, list):
        cutoff = date.today() - timedelta(days=240)
        form4_idxs = []
        for i, form in enumerate(forms):
            if str(form).upper() != '4':
                continue
            fd = filing_dates[i] if isinstance(filing_dates, list) and i < len(filing_dates) else None
            fd_parsed = _parse_iso_date(fd)
            if fd_parsed:
                try:
                    if datetime.strptime(fd_parsed, '%Y-%m-%d').date() >= cutoff:
                        form4_idxs.append(i)
                except Exception:
                    pass
        out['sec_recent_form4_count_240d'] = len(form4_idxs)
        if form4_idxs:
            def _form4_idx_sort_key(idx: int) -> str:
                if isinstance(filing_dates, list) and idx < len(filing_dates):
                    return str(_parse_iso_date(filing_dates[idx]) or '0000-00-00')
                return '0000-00-00'
            form4_idxs = sorted(form4_idxs, key=_form4_idx_sort_key, reverse=True)
            i = form4_idxs[0]
            accession = accessions[i] if isinstance(accessions, list) and i < len(accessions) else None
            primary_doc = primary_docs[i] if isinstance(primary_docs, list) and i < len(primary_docs) else None
            out['sec_last_form4_accession'] = accession
            if sec_cik and accession and primary_doc:
                try:
                    _cik_int = str(int(str(sec_cik).strip()))
                except (ValueError, TypeError):
                    _cik_int = str(sec_cik).strip().lstrip('0') or str(sec_cik).strip()
                out['sec_last_form4_url'] = f"https://www.sec.gov/Archives/edgar/data/{_cik_int}/{str(accession).replace('-', '')}/{primary_doc}"
        if latest_insider_filing_date is not None:
            try:
                insider_dt = datetime.strptime(latest_insider_filing_date, '%Y-%m-%d').date()
            except Exception:
                insider_dt = None
            if insider_dt is not None and isinstance(filing_dates, list):
                offering_forms = {'S-1', 'S-3', '424B4'}
                found = False
                for i, form in enumerate(forms):
                    if str(form).upper() not in offering_forms:
                        continue
                    fd = filing_dates[i] if i < len(filing_dates) else None
                    fd_parsed = _parse_iso_date(fd)
                    if not fd_parsed:
                        continue
                    try:
                        if datetime.strptime(fd_parsed, '%Y-%m-%d').date() >= insider_dt:
                            found = True
                            break
                    except Exception:
                        pass
                out['sec_secondary_offering_flag'] = found
    return out

def parse_manual_review_section(identity: dict[str, Any], profitability: dict[str, Any], insider: dict[str, Any], solvency: dict[str, Any]) -> dict[str, Any]:
    out = _none_dict(['manual_review_chs_required', 'manual_review_path_to_positive_fcf', 'manual_review_ceo_cfo_self_sale', 'manual_review_secondary_offering_after_buy', 'manual_review_correlation_to_top3', 'manual_review_chinese_adr_dual_listing', 'manual_review_dtc_independence', 'manual_review_holdings_verified', 'manual_review_sector_etf_representative', 'manual_review_portfolio_context', 'manual_review_regime_state', 'manual_review_notes'])
    out['manual_review_chs_required'] = identity.get('altman_model_variant') == 'chs_only'
    out['manual_review_path_to_positive_fcf'] = profitability.get('free_cash_flow_negative_3y_streak') is True
    out['manual_review_ceo_cfo_self_sale'] = bool(insider.get('_ceo_cfo_self_sale_within_90d')) if insider.get('_ceo_cfo_self_sale_within_90d') is not None else None
    out['manual_review_secondary_offering_after_buy'] = None
    out['manual_review_correlation_to_top3'] = None
    out['manual_review_chinese_adr_dual_listing'] = identity.get('chinese_adr_flag') is True
    out['manual_review_dtc_independence'] = solvency.get('dtc_7_15_modifier_flag') is True
    out['manual_review_holdings_verified'] = insider.get('insider_holdings_verified') is False
    out['manual_review_sector_etf_representative'] = identity.get('sector') is None or _sector_etf_for_sector(identity.get('sector')) is None
    out['manual_review_portfolio_context'] = None
    out['manual_review_regime_state'] = None
    return out

def parse_audit_section(run_id: str, run_mode: str, source_market_data: str, source_quote: str, source_profile: str, source_cashflow: str, source_scores: str, source_analyst: str, source_insider: str, source_sec: str, row: dict[str, Any]) -> dict[str, Any]:
    out = _none_dict(['script_version', 'run_id', 'run_mode', 'source_market_data', 'source_quote', 'source_profile', 'source_cashflow', 'source_scores', 'source_analyst', 'source_insider', 'source_sec', 'null_fields'])
    out['script_version'] = SCRIPT_VERSION
    out['run_id'] = run_id
    out['run_mode'] = run_mode
    out['source_market_data'] = source_market_data
    out['source_quote'] = source_quote
    out['source_profile'] = source_profile
    out['source_cashflow'] = source_cashflow
    out['source_scores'] = source_scores
    out['source_analyst'] = source_analyst
    out['source_insider'] = source_insider
    out['source_sec'] = source_sec
    merged = dict(row)
    merged.update(out)
    out['null_fields'] = '|'.join([k for k in OUTPUT_COLUMNS if k != 'null_fields' and merged.get(k) is None])
    return out
MIN_MARKET_CAP = 50000000.0
MAX_PCT_BELOW_200SMA = -30.0
PS_HARD_FLOOR_CEILING = 30.0
ALTMAN_1968_MIN_PASS = 1.81
ALTMAN_ZPP_MIN_PASS = 1.1
PIOTROSKI_MIN_PASS = 4.0
MAX_DTC_HARD_FAIL = 15.0

def _normalize_text(value: Any) -> Optional[str]:
    return None if value is None else str(value).strip().lower()

def _get_tier_threshold(config: dict[str, Optional[float]], tier: Optional[str]) -> Optional[float]:
    return None if tier is None else config.get(str(tier))

def _classify_chs_result(chs_result: Any) -> Optional[bool]:
    norm = _normalize_text(chs_result)
    if norm is None:
        return None
    if norm in CHS_PASS_VALUES:
        return True
    if norm in CHS_FAIL_VALUES:
        return False
    return None
STACKING_FAIL_THRESHOLD = 3
STACKING_DTC_MIN = 7.0
STACKING_DTC_MAX = 15.0
STACKING_SI_MIN = 20.0
STACKING_HV30_MIN = 40.0
STACKING_SIGNAL_AGE_MIN = 120
STACKING_SIGNAL_AGE_MAX = 240
STACKING_PRICE_MIN = -30.0
STACKING_PRICE_MAX = -15.0
STACKING_EQ_FAIL_MAX = -2.0
SMALL_TIER_HOLDINGS_FALLBACK_VALUE_MAX = 50000.0
SMALL_TIER_ALTMAN_STACKING_MAX = 2.0


def _fallback_threshold_for_tier(tier: Optional[str]) -> Optional[float]:
    """Holdings fallback dollar thresholds per v6.8: Large $500K, Mid $250K, Small $100K."""
    return {'Large': 500000.0, 'Mid': 250000.0, 'Small': 100000.0}.get(str(tier) if tier else '', None)

def _normalize_text_stack(value: Any) -> str | None:
    return None if value is None else str(value).strip().lower()

def apply_stacking_rule(row: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    out = {'stacking_condition_1_dtc': None, 'stacking_condition_2_si': None, 'stacking_condition_3_vol': None, 'stacking_condition_4_holdings': None, 'stacking_condition_5_signal_age': None, 'stacking_condition_6_eq': None, 'stacking_condition_7_conflict': None, 'stacking_condition_8_price': None, 'stacking_count': None, 'stacking_result': None}
    incomplete_reasons: list[str] = []
    market_cap_tier = row.get('market_cap_tier')
    days_to_cover = row.get('days_to_cover')
    dtc_independence_check = row.get('dtc_independence_check')
    if days_to_cover is None:
        incomplete_reasons.append('missing_days_to_cover_for_stacking')
    else:
        in_window = STACKING_DTC_MIN <= float(days_to_cover) <= STACKING_DTC_MAX
        if in_window:
            if dtc_independence_check is None:
                incomplete_reasons.append('missing_dtc_independence_check_for_stacking')
            else:
                out['stacking_condition_1_dtc'] = str(dtc_independence_check).strip().lower() != 'waived'
        else:
            out['stacking_condition_1_dtc'] = False
    si_pct_at_filing = row.get('si_pct_at_filing')
    cluster_buy_flag = row.get('cluster_buy_flag')
    if si_pct_at_filing is None:
        incomplete_reasons.append('missing_si_pct_at_filing_for_stacking')
    elif cluster_buy_flag is None:
        incomplete_reasons.append('missing_cluster_buy_flag_for_stacking')
    else:
        out['stacking_condition_2_si'] = float(si_pct_at_filing) > STACKING_SI_MIN and cluster_buy_flag is not True
    hv30_annualized = row.get('hv30_annualized')
    if hv30_annualized is None:
        incomplete_reasons.append('missing_hv30_annualized_for_stacking')
    else:
        out['stacking_condition_3_vol'] = float(hv30_annualized) > STACKING_HV30_MIN
    insider_holdings_verified = row.get('insider_holdings_verified')
    insider_fallback_threshold_used = row.get('insider_fallback_threshold_used')
    latest_insider_value = row.get('latest_insider_value')
    if insider_holdings_verified is None:
        incomplete_reasons.append('missing_insider_holdings_verified_for_stacking')
    if insider_fallback_threshold_used is None:
        incomplete_reasons.append('missing_insider_fallback_threshold_used_for_stacking')
    if insider_holdings_verified is not None and insider_fallback_threshold_used is not None:
        if insider_holdings_verified is False and insider_fallback_threshold_used is True:
            if market_cap_tier == 'Small':
                if latest_insider_value is None:
                    incomplete_reasons.append('missing_latest_insider_value_for_small_tier_holdings_stacking')
                else:
                    out['stacking_condition_4_holdings'] = float(latest_insider_value) < SMALL_TIER_HOLDINGS_FALLBACK_VALUE_MAX
            elif market_cap_tier in {'Large', 'Mid'}:
                threshold = _fallback_threshold_for_tier(market_cap_tier)
                if threshold is None:
                    incomplete_reasons.append(f'missing_threshold_for_{str(market_cap_tier).lower()}_tier_holdings_stacking')
                else:
                    out['stacking_condition_4_holdings'] = True
            else:
                out['stacking_condition_4_holdings'] = True
        else:
            out['stacking_condition_4_holdings'] = False
    signal_age_days = row.get('signal_age_days')
    if signal_age_days is None:
        incomplete_reasons.append('missing_signal_age_days_for_stacking')
    else:
        out['stacking_condition_5_signal_age'] = STACKING_SIGNAL_AGE_MIN <= int(signal_age_days) <= STACKING_SIGNAL_AGE_MAX
    eq_cumulative_score = row.get('stacking_eq_cumulative_score')
    if eq_cumulative_score is None:
        incomplete_reasons.append('missing_stacking_eq_cumulative_score')
    else:
        out['stacking_condition_6_eq'] = float(eq_cumulative_score) <= STACKING_EQ_FAIL_MAX
    altman_piotroski_conflict_flag = row.get('altman_piotroski_conflict_flag')
    chs_result = row.get('chs_result')
    altman_variant = row.get('altman_model_variant')
    altman_z_score = row.get('altman_z_score')
    base_conflict: bool | None = None
    if altman_variant == 'chs_only':
        norm = _normalize_text_stack(chs_result)
        if norm is None:
            incomplete_reasons.append('missing_chs_result_for_stacking_conflict')
        elif norm in CHS_PASS_VALUES or norm in CHS_FAIL_VALUES:
            base_conflict = False
        else:
            base_conflict = True
    elif altman_variant is None:
        incomplete_reasons.append('missing_altman_model_variant_for_stacking_conflict')
    elif altman_piotroski_conflict_flag is None:
        incomplete_reasons.append('missing_altman_piotroski_conflict_flag_for_stacking')
    else:
        base_conflict = bool(altman_piotroski_conflict_flag)
    small_tier_altman_flag: bool | None = False
    if market_cap_tier == 'Small' and altman_variant != 'chs_only':
        if altman_z_score is None:
            small_tier_altman_flag = None
            incomplete_reasons.append('missing_altman_z_score_for_small_tier_stacking')
        else:
            small_tier_altman_flag = float(altman_z_score) < SMALL_TIER_ALTMAN_STACKING_MAX and chs_result is None
    out['stacking_condition_7_conflict'] = None if base_conflict is None or small_tier_altman_flag is None else bool(base_conflict or small_tier_altman_flag)
    pct_vs_200sma = row.get('pct_vs_200sma')
    if pct_vs_200sma is None:
        incomplete_reasons.append('missing_pct_vs_200sma_for_stacking')
    else:
        p = float(pct_vs_200sma)
        out['stacking_condition_8_price'] = p > STACKING_PRICE_MIN and p <= STACKING_PRICE_MAX
    condition_keys = [k for k in out.keys() if k.startswith('stacking_condition_')]
    true_count = sum((1 for k in condition_keys if out[k] is True))
    any_null = any((out[k] is None for k in condition_keys))
    out['stacking_count'] = true_count
    out['stacking_result'] = 'FAIL' if true_count >= STACKING_FAIL_THRESHOLD else 'INCOMPLETE' if any_null else 'PASS'
    return (out['stacking_result'], out)
CCG_PASS_MIN = 60.0
CCG_FULL_WEIGHT = 0.25
CCG_ONE_THIRD_WEIGHT = 1.0 / 3.0

def apply_ccg(row: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    out = {'ccg_momentum_score': row.get('f1_momentum_adjusted'), 'ccg_profitability_score': row.get('f3_profitability_adjusted'), 'ccg_earnings_quality_score': row.get('f2_earnings_quality_adjusted'), 'ccg_ear_score': row.get('f4_ear_adjusted'), 'ccg_ear_exempt': row.get('ear_neutral_flag'), 'ccg_weight_used': None, 'core_conviction_score': None, 'ccg_result': None}
    f1 = row.get('f1_momentum_adjusted')
    f2 = row.get('f2_earnings_quality_adjusted')
    f3 = row.get('f3_profitability_adjusted')
    f4 = row.get('f4_ear_adjusted')
    ear_neutral_flag = row.get('ear_neutral_flag')
    if ear_neutral_flag is None:
        out['ccg_result'] = 'INCOMPLETE'
        return (out['ccg_result'], out)
    if ear_neutral_flag is True:
        if any((v is None for v in [f1, f2, f3])):
            out['ccg_weight_used'] = 'one_third_each_ex_ear'
            out['ccg_result'] = 'INCOMPLETE'
            return (out['ccg_result'], out)
        out['ccg_weight_used'] = 'one_third_each_ex_ear'
        out['core_conviction_score'] = (float(f1) * CCG_ONE_THIRD_WEIGHT + float(f2) * CCG_ONE_THIRD_WEIGHT + float(f3) * CCG_ONE_THIRD_WEIGHT) * 10.0
    else:
        if any((v is None for v in [f1, f2, f3, f4])):
            out['ccg_weight_used'] = '0.25_each'
            out['ccg_result'] = 'INCOMPLETE'
            return (out['ccg_result'], out)
        out['ccg_weight_used'] = '0.25_each'
        out['core_conviction_score'] = (float(f1) * CCG_FULL_WEIGHT + float(f2) * CCG_FULL_WEIGHT + float(f3) * CCG_FULL_WEIGHT + float(f4) * CCG_FULL_WEIGHT) * 10.0
    out['ccg_result'] = 'PASS' if out['core_conviction_score'] >= CCG_PASS_MIN else 'FAIL'
    return (out['ccg_result'], out)
CHS_FAIL_VALUES = {'fail', 'distress', 'weak', 'red', 'high_risk', 'high-risk'}
CHS_PASS_VALUES = {'pass', 'safe', 'ok', 'green', 'low_risk', 'low-risk'}

def _clamp_score(value: Optional[float], lo: float=0.0, hi: float=10.0) -> Optional[float]:
    return None if value is None else max(lo, min(hi, float(value)))

def _weighted(score: Optional[float], weight: float) -> Optional[float]:
    return None if score is None else float(score) * weight

def _bool(v: Any) -> bool:
    return v is True

def _role_is_ceo_cfo(title: Optional[str]) -> bool:
    t = (title or '').lower()
    return any((x in t for x in ['chief executive', 'ceo', 'chief financial', 'cfo']))

def _role_is_director(title: Optional[str]) -> bool:
    t = (title or '').lower()
    return 'director' in t and (not _role_is_ceo_cfo(title))

def _factor_result(raw: Optional[float], adjusted: Optional[float], weight: float, raw_adjusted_prefix: str, weighted_key: str) -> dict[str, Any]:
    return {f'{raw_adjusted_prefix}_raw': raw, f'{raw_adjusted_prefix}_adjusted': adjusted, weighted_key: _weighted(adjusted, weight)}

def score_factor_2(row: dict[str, Any]) -> dict[str, Any]:
    sector = (row.get('sector') or '').lower()
    industry = (row.get('industry') or '').lower()
    sloan = row.get('sloan_ratio')
    ocf_ni = row.get('ocf_ni_ratio')
    ocf_rev = row.get('ocf_revenue_ratio')
    sector_ocf_rev_median = row.get('sector_ocf_revenue_median')
    etr_change = row.get('etr_change_qoq')
    ar_vs_rev = row.get('ar_growth_vs_revenue')
    inv_vs_rev = row.get('inventory_growth_vs_revenue')
    asset_growth = row.get('asset_growth_yoy')
    net_dilution = row.get('net_dilution_pct')
    persistent_div = row.get('persistent_ocf_ni_divergence_flag')
    rev_earn_div = row.get('revenue_vs_earnings_divergence_flag')
    net_income = row.get('net_income_ttm')
    if sloan is None:
        return {**_factor_result(None, None, 0.2, 'f2_earnings_quality', 'f2_weighted'), '_stacking_eq_cumulative_score': None, 'sector_sloan_threshold': None}
    sloan_pct = float(sloan) * 100.0 if abs(float(sloan)) <= 2 else float(sloan)
    tech_like = any((x in sector for x in ['technology', 'communication'])) or any((x in industry for x in ['software', 'internet', 'saas', 'asset-light']))
    reit_utility_like = 'reit' in sector or 'reit' in industry or 'utilities' in sector or ('mlp' in industry)
    sector_sloan_threshold = 5.0 if tech_like else 8.0
    soft_flags = 0
    if etr_change is not None and float(etr_change) < -5.0:
        soft_flags += 1
    if ar_vs_rev is not None and float(ar_vs_rev) > 0 or (inv_vs_rev is not None and float(inv_vs_rev) > 0):
        soft_flags += 1
    if asset_growth is not None and float(asset_growth) > 20.0:
        soft_flags += 1
    if net_dilution is not None and float(net_dilution) > 3.0:
        soft_flags += 1
    if persistent_div is True:
        soft_flags += 1
    ni_negative = net_income is not None and float(net_income) < 0
    quality_met = None
    if ni_negative:
        if ocf_rev is not None and sector_ocf_rev_median is not None:
            quality_met = float(ocf_rev) >= float(sector_ocf_rev_median)
    elif reit_utility_like:
        if ocf_rev is not None:
            quality_met = float(ocf_rev) > 0.15
    elif tech_like:
        if ocf_ni is not None:
            quality_met = float(ocf_ni) >= 0.7
    elif ocf_ni is not None:
        quality_met = float(ocf_ni) >= 0.85
    if ni_negative:
        q10_ok = quality_met is True
        q8_ok = quality_met is True
        q5_ok = quality_met is True
    elif reit_utility_like:
        q10_ok = quality_met is True
        q8_ok = quality_met is True
        q5_ok = quality_met is True
    else:
        q10_ok = ocf_ni is not None and float(ocf_ni) > 1.0
        q8_ok = ocf_ni is not None and 0.8 <= float(ocf_ni) <= 1.0
        q5_ok = ocf_ni is not None and 0.6 <= float(ocf_ni) <= 0.8
    if sloan_pct < 3.0 and q10_ok and (soft_flags == 0):
        raw = 10.0
    elif (((tech_like and 3.0 <= sloan_pct <= 5.0) or ((not tech_like) and 3.0 <= sloan_pct <= 8.0)) and q8_ok and (soft_flags == 0)):
        raw = 8.0
    elif sloan_pct <= sector_sloan_threshold and q5_ok and (soft_flags == 1):
        raw = 5.0
    elif sloan_pct > sector_sloan_threshold:
        raw = 2.0
    else:
        raw = 1.0
    penalty = soft_flags * 0.5
    eq_cumulative_score = -penalty
    if rev_earn_div is True:
        penalty += 2.0
        eq_cumulative_score -= 2.0
    adjusted = _clamp_score(raw - penalty, 0.0, 10.0)
    return {**_factor_result(raw, adjusted, 0.2, 'f2_earnings_quality', 'f2_weighted'), '_stacking_eq_cumulative_score': eq_cumulative_score, 'sector_sloan_threshold': sector_sloan_threshold}

def score_factor_4(row: dict[str, Any]) -> dict[str, Any]:
    ear = row.get('ear_3d_abnormal_return')
    revenue_beat = row.get('ear_revenue_beat')
    eps_beat = row.get('ear_eps_beat')
    td_since = row.get('trading_days_since_earnings')
    ear_neutral = row.get('ear_neutral_flag')
    analyst_count = row.get('analyst_count')
    rev_pct = row.get('revision_momentum_pct')
    rev_dir = row.get('revision_direction')
    if ear_neutral is True:
        raw = 4.0
    else:
        if ear is None or td_since is None:
            return _factor_result(None, None, 0.1, 'f4_ear', 'f4_weighted')
        e = float(ear)
        t = int(td_since)
        if e > 5.0 and revenue_beat is True and (t <= 30):
            raw = 10.0
        elif e > 3.0 and revenue_beat is True and (t <= 45):
            raw = 8.0
        elif e > 3.0 and revenue_beat is True and (t <= 63):
            raw = 6.0
        elif 0.0 < e <= 3.0 and revenue_beat is True and (t <= 63):
            raw = 6.0
        elif e > 0.0 and revenue_beat is False and (t <= 63):
            raw = 5.0
        elif e == 0.0 and t <= 63:
            raw = 4.0
        elif e < 0.0:
            raw = 1.0
        elif t > 63:
            raw = 4.0
        else:
            raw = None
        if raw is not None and eps_beat is True and (revenue_beat is False):
            raw = min(raw, 5.0)
    if raw is None:
        return _factor_result(None, None, 0.1, 'f4_ear', 'f4_weighted')
    modifiers = 0.0
    if analyst_count is not None and int(analyst_count) >= 2:
        if rev_dir == 'up' and rev_pct is not None and (float(rev_pct) >= 5.0):
            modifiers += 1.0
        elif rev_dir == 'down' and rev_pct is not None and (float(rev_pct) <= -5.0):
            modifiers -= 1.0
    adjusted = _clamp_score(raw + modifiers)
    if eps_beat is True and revenue_beat is False and adjusted is not None:
        adjusted = min(adjusted, 5.0)
    return _factor_result(raw, adjusted, 0.1, 'f4_ear', 'f4_weighted')

def score_factor_6(row: dict[str, Any]) -> dict[str, Any]:
    peg = row.get('peg_ratio')
    analyst_count = row.get('analyst_count')
    market_cap_tier = row.get('market_cap_tier')
    ev = row.get('ev_ebitda')
    ev_med = row.get('ev_ebitda_sector_median')
    gpa = row.get('gpa_ratio')
    gpa_med = row.get('gpa_sector_median')
    net_cash = row.get('net_cash_flag')
    soft_flag = row.get('valuation_soft_flag')
    net_debt_ebitda = row.get('net_debt_ebitda')
    sector = (row.get('sector') or '').lower()
    ev_only = market_cap_tier == 'Mid' and (analyst_count is None or int(analyst_count) < 2)
    if market_cap_tier == 'Small':
        ev_only = analyst_count is None or int(analyst_count) < 3
    if ev is None or ev_med is None:
        return _factor_result(None, None, 0.05, 'f6_valuation', 'f6_weighted')
    ev_ratio = float(ev) / float(ev_med) if float(ev_med) != 0 else None
    if ev_ratio is None:
        return _factor_result(None, None, 0.05, 'f6_valuation', 'f6_weighted')
    if ev_only:
        raw = 8.0 if float(ev) < float(ev_med) else 6.0 if float(ev) == float(ev_med) else 4.0
    else:
        if peg is None:
            return _factor_result(None, None, 0.05, 'f6_valuation', 'f6_weighted')
        p = float(peg)
        gpa_above = gpa is not None and gpa_med is not None and (float(gpa) > float(gpa_med))
        if p < 1.0 and ev_ratio < 1.0 and gpa_above:
            raw = 10.0
        elif 1.0 <= p <= 1.5 and ev_ratio <= 1.0:
            raw = 8.0
        elif 1.5 <= p <= 2.0 and ev_ratio <= 1.5:
            raw = 6.0
        elif 2.0 <= p <= 2.5 and 1.5 <= ev_ratio <= 2.0:
            raw = 4.0
        elif p > 2.5 or ev_ratio > 2.0:
            raw = 2.0
        else:
            raw = None
    if raw is None:
        return _factor_result(None, None, 0.05, 'f6_valuation', 'f6_weighted')
    adjusted = raw
    if gpa is not None and gpa_med is not None and (float(gpa) > float(gpa_med)):
        adjusted += 0.5
    if net_cash is True:
        adjusted += 0.5
    if soft_flag is True:
        adjusted -= 1.0
    sector_normal = 'utilities' in sector or 'reit' in sector or 'real estate' in sector
    if net_debt_ebitda is not None and float(net_debt_ebitda) > 4.0 and (not sector_normal):
        adjusted -= 1.0
    return _factor_result(raw, _clamp_score(adjusted), 0.05, 'f6_valuation', 'f6_weighted')

def _piotroski_band_for_solvency(piotroski: Optional[float], market_cap: Optional[float]) -> Optional[float]:
    if piotroski is None:
        return None
    p = float(piotroski)
    if market_cap is not None and float(market_cap) > 10000000000 and (p == 6):
        return None
    if p >= 8:
        return 10.0
    if p == 7:
        return 8.0
    if p in (5.0, 6.0):
        return 5.0
    if p == 4.0:
        return 2.0
    return None

def _altman_band_for_solvency(altman: Optional[float], altman_variant: Optional[str]) -> Optional[float]:
    if altman is None:
        return None
    a = float(altman)
    if altman_variant == '1995_z_double_prime':
        if a > 2.6:
            return 10.0
        if 1.1 <= a <= 2.6:
            return 5.0
        return 2.0
    if a > 2.99:
        return 10.0
    if 2.67 <= a <= 2.99:
        return 8.0
    if 2.0 <= a < 2.67:
        return 5.0
    if a < 2.0:
        return 2.0
    return None
TIER_A_FLOOR_SCORE = 80.0
TIER_B_FLOOR_SCORE = 65.0
TIER_C_FLOOR_SCORE = 50.0
TIER_D_FLOOR_SCORE = 35.0
TIER_A_FLOOR_PCT = 6.0
TIER_A_RANGE_PCT = 2.0
TIER_A_SCORE_RANGE = 20.0
TIER_A_CAP = 8.0
TIER_B_FLOOR_PCT = 3.0
TIER_B_RANGE_PCT = 2.0
TIER_B_CAP = 5.0
TIER_C_FLOOR_PCT = 1.0
TIER_C_RANGE_PCT = 1.0
TIER_C_SCORE_RANGE = 14.0
TIER_C_CAP = 2.0
SMALL_TIER_HARD_CAP = 1.0
STANDALONE_VOL_DEDUCTION = -5.0
STANDALONE_SI_DTC_DEDUCTION = -1.0
STANDALONE_EQ_DEDUCTION = -5.0
STANDALONE_CONCENTRATION_DEDUCTION = -3.0
STANDALONE_RATES_DEDUCTION = -3.0
REGIME_FULL_CRASH_DEDUCTION = -20.0
REGIME_MTUM_VTV_DEDUCTION = -5.0
REGIME_HY_OAS_MOMENTUM_DEDUCTION = -5.0
OVERRIDE_2_CAP_SCORE = 79.0
SWING_MAX_STOP_PCT = 7.0
LONG_TERM_MAX_STOP_PCT = 15.0

def _sum_if_all_present(values: list[Optional[float]]) -> Optional[float]:
    return None if any((v is None for v in values)) else float(sum((float(v) for v in values)))

def _tier_from_composite(score: Optional[float]) -> Optional[str]:
    if score is None:
        return None
    s = float(score)
    if s >= 80.0:
        return 'A'
    if s >= 65.0:
        return 'B'
    if s >= 50.0:
        return 'C'
    if s >= 35.0:
        return 'D Watch'
    return 'Eliminate'

def _tier_floor_score(tier: Optional[str]) -> Optional[float]:
    return TIER_A_FLOOR_SCORE if tier == 'A' else TIER_B_FLOOR_SCORE if tier == 'B' else TIER_C_FLOOR_SCORE if tier == 'C' else TIER_D_FLOOR_SCORE if tier == 'D Watch' else None

def _tier_floor_pct(tier: Optional[str]) -> Optional[float]:
    return TIER_A_FLOOR_PCT if tier == 'A' else TIER_B_FLOOR_PCT if tier == 'B' else TIER_C_FLOOR_PCT if tier == 'C' else None

def _tier_range_pct(tier: Optional[str]) -> Optional[float]:
    return TIER_A_RANGE_PCT if tier == 'A' else TIER_B_RANGE_PCT if tier == 'B' else TIER_C_RANGE_PCT if tier == 'C' else None

def _tier_score_range(tier: Optional[str]) -> Optional[float]:
    return TIER_A_SCORE_RANGE if tier == 'A' else TIER_B_SCORE_RANGE if tier == 'B' else TIER_C_SCORE_RANGE if tier == 'C' else None

def _tier_abs_cap(tier: Optional[str]) -> Optional[float]:
    return TIER_A_CAP if tier == 'A' else TIER_B_CAP if tier == 'B' else TIER_C_CAP if tier == 'C' else None

def _within_pct_of_52w_high(current_price: Optional[float], high_52w: Optional[float], threshold_pct: float) -> Optional[bool]:
    if current_price is None or high_52w in (None, 0):
        return None
    return (1.0 - float(current_price) / float(high_52w)) * 100.0 <= threshold_pct

def _max_stop_pct_from_trade_type(trade_type: Optional[str]) -> Optional[float]:
    if trade_type is None:
        return None
    t = str(trade_type).strip().lower()
    return SWING_MAX_STOP_PCT if t == 'swing' else LONG_TERM_MAX_STOP_PCT if t in {'long-term', 'long_term', 'longterm'} else None

def _cap_tier_to_c_if_above(tier: Optional[str]) -> Optional[str]:
    return 'C' if tier in {'A', 'B'} else tier
CHECKPOINT_FILENAME = 'phase1_checkpoint.json'

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

def _output_columns_signature() -> str:
    return _sha256_text('\n'.join(OUTPUT_COLUMNS))

def _checkpoint_path(output_dir: str) -> str:
    return os.path.join(output_dir, CHECKPOINT_FILENAME)

def _atomic_write_json(path: str, payload: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', delete=False, dir=os.path.dirname(path), suffix='.tmp') as tmp:
        json.dump(payload, tmp, indent=2, ensure_ascii=False)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_path = tmp.name
    os.replace(tmp_path, path)

def _normalize_ticker(ticker: str) -> str:
    return str(ticker).strip().upper()

def _result_bucket_for_row(row: dict[str, Any]) -> str:
    for key in ('hard_floor_result', 'stacking_result', 'ccg_result'):
        value = row.get(key)
        if value in {'PASS', 'FAIL', 'INCOMPLETE'}:
            return value
    return 'UNKNOWN'

def _recompute_checkpoint_counts(rows_by_ticker: dict[str, dict[str, Any]]) -> dict[str, int]:
    counts = {'PASS': 0, 'FAIL': 0, 'INCOMPLETE': 0, 'UNKNOWN': 0}
    for _, row in rows_by_ticker.items():
        counts[_result_bucket_for_row(row)] += 1
    return counts

def prepare_row_for_persist(row: dict[str, Any], checkpoint: dict[str, Any]) -> dict[str, Any]:
    normalized = {col: row.get(col) for col in OUTPUT_COLUMNS}
    normalized['script_version'] = SCRIPT_VERSION
    normalized['run_id'] = checkpoint['run_id']
    normalized['run_mode'] = checkpoint['run_mode']
    normalized['null_fields'] = '|'.join([col for col in OUTPUT_COLUMNS if col != 'null_fields' and normalized.get(col) is None])
    return normalized

def _new_checkpoint(run_id: str, run_mode: str, expected_tickers: list[str], output_dir: str) -> dict[str, Any]:
    now = _utc_now_iso()
    ticker_list = [_normalize_ticker(t) for t in expected_tickers]
    return {'script_version': SCRIPT_VERSION, 'output_columns_signature': _output_columns_signature(), 'run_id': run_id, 'run_mode': run_mode, 'created_at_utc': now, 'last_updated_at_utc': now, 'output_dir': output_dir, 'ticker_universe': ticker_list, 'ticker_universe_signature': _sha256_text('\n'.join(ticker_list)), 'completed_tickers': [], 'rows_by_ticker': {}, 'counts': {'PASS': 0, 'FAIL': 0, 'INCOMPLETE': 0, 'UNKNOWN': 0}, 'notes': []}

def load_or_init_checkpoint(output_dir: str, run_id: str, run_mode: str, expected_tickers: list[str], force: bool=False) -> dict[str, Any]:
    os.makedirs(output_dir, exist_ok=True)
    path = _checkpoint_path(output_dir)
    current_universe_sig = _sha256_text('\n'.join([_normalize_ticker(t) for t in expected_tickers]))
    if not os.path.exists(path):
        cp = _new_checkpoint(run_id, run_mode, expected_tickers, output_dir)
        _atomic_write_json(path, cp)
        return cp
    with open(path, 'r', encoding='utf-8') as f:
        cp = json.load(f)
    if cp.get('script_version') != SCRIPT_VERSION and (not force):
        raise RuntimeError('Checkpoint version mismatch. Use --force to override.')
    if cp.get('output_columns_signature') != _output_columns_signature() and (not force):
        raise RuntimeError('Checkpoint output schema mismatch. Use --force to override.')
    if cp.get('run_mode') != run_mode and (not force):
        raise RuntimeError('Checkpoint run_mode mismatch. Use --force to override.')
    if cp.get('ticker_universe_signature') != current_universe_sig and (not force):
        raise RuntimeError('Checkpoint ticker universe mismatch. Use --force to override.')
    cp.setdefault('completed_tickers', [])
    cp.setdefault('rows_by_ticker', {})
    cp.setdefault('counts', {'PASS': 0, 'FAIL': 0, 'INCOMPLETE': 0, 'UNKNOWN': 0})
    cp.setdefault('notes', [])
    cp['run_id'] = cp.get('run_id') or run_id
    cp['run_mode'] = run_mode
    cp['output_dir'] = output_dir
    cp['last_updated_at_utc'] = _utc_now_iso()
    cp['script_version'] = SCRIPT_VERSION
    cp['output_columns_signature'] = _output_columns_signature()
    cp['ticker_universe'] = [_normalize_ticker(t) for t in expected_tickers]
    cp['ticker_universe_signature'] = current_universe_sig
    cp['counts'] = _recompute_checkpoint_counts(cp['rows_by_ticker'])
    _atomic_write_json(path, cp)
    return cp

def checkpoint_has_ticker(checkpoint: dict[str, Any], ticker: str) -> bool:
    return _normalize_ticker(ticker) in set(checkpoint.get('completed_tickers', []))

def get_remaining_tickers(expected_tickers: list[str], checkpoint: dict[str, Any]) -> list[str]:
    completed = set((_normalize_ticker(t) for t in checkpoint.get('completed_tickers', [])))
    return [_normalize_ticker(t) for t in expected_tickers if _normalize_ticker(t) not in completed]

def save_completed_ticker_to_checkpoint(output_dir: str, checkpoint: dict[str, Any], ticker: str, row: dict[str, Any]) -> dict[str, Any]:
    t = _normalize_ticker(ticker)
    if checkpoint_has_ticker(checkpoint, t):
        raise RuntimeError(f'Refusing to reprocess ticker already completed in checkpoint: {t}')
    normalized_row = prepare_row_for_persist(row, checkpoint)
    checkpoint['rows_by_ticker'][t] = normalized_row
    checkpoint['completed_tickers'].append(t)
    checkpoint['completed_tickers'] = sorted(set(checkpoint['completed_tickers']))
    checkpoint['counts'] = _recompute_checkpoint_counts(checkpoint['rows_by_ticker'])
    checkpoint['last_updated_at_utc'] = _utc_now_iso()
    _atomic_write_json(_checkpoint_path(output_dir), checkpoint)
    return checkpoint

def checkpoint_completed_rows_in_universe_order(checkpoint: dict[str, Any], expected_tickers: list[str]) -> list[dict[str, Any]]:
    rows_by_ticker = checkpoint.get('rows_by_ticker', {})
    out = []
    for t in expected_tickers:
        row = rows_by_ticker.get(_normalize_ticker(t))
        if row is not None:
            out.append(row)
    return out

def checkpoint_summary(checkpoint: dict[str, Any]) -> dict[str, Any]:
    completed = checkpoint.get('completed_tickers', [])
    universe = checkpoint.get('ticker_universe', [])
    counts = checkpoint.get('counts', {})
    return {'run_id': checkpoint.get('run_id'), 'run_mode': checkpoint.get('run_mode'), 'script_version': checkpoint.get('script_version'), 'created_at_utc': checkpoint.get('created_at_utc'), 'last_updated_at_utc': checkpoint.get('last_updated_at_utc'), 'completed_count': len(completed), 'remaining_count': max(len(universe) - len(completed), 0), 'counts': counts}
EXTRACTOR_ALL_FILENAME = 'extractor_all.csv'
SURVIVORS_FILENAME = 'survivors.csv'
ELIMINATED_FILENAME = 'eliminated.csv'
INCOMPLETE_FILENAME = 'incomplete.csv'
RUN_MANIFEST_FILENAME = 'run_manifest.json'
PROVIDER_SUMMARY_FILENAME = 'provider_summary.json'
SCHEMA_CHECK_FILENAME = 'schema_check.txt'
DEFAULT_SMOKE_TICKER_COUNT = 3

def _utc_now_iso_manifest() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def _manifest_path(output_dir: str) -> str:
    return os.path.join(output_dir, RUN_MANIFEST_FILENAME)

def _provider_summary_path(output_dir: str) -> str:
    return os.path.join(output_dir, PROVIDER_SUMMARY_FILENAME)

def _schema_check_path(output_dir: str) -> str:
    return os.path.join(output_dir, SCHEMA_CHECK_FILENAME)

def _extractor_all_path(output_dir: str) -> str:
    return os.path.join(output_dir, EXTRACTOR_ALL_FILENAME)

def _survivors_path(output_dir: str) -> str:
    return os.path.join(output_dir, SURVIVORS_FILENAME)

def _eliminated_path(output_dir: str) -> str:
    return os.path.join(output_dir, ELIMINATED_FILENAME)

def _incomplete_path(output_dir: str) -> str:
    return os.path.join(output_dir, INCOMPLETE_FILENAME)

def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description='v6.8 full extractor')
    p.add_argument('--output-dir', type=str, default='phase1_output')
    p.add_argument('--full-universe', action='store_true')
    p.add_argument('--tickers', type=str, default=None)
    p.add_argument('--force', action='store_true')
    p.add_argument('--allow-full-without-smoke', action='store_true')
    return p

def parse_cli_args() -> argparse.Namespace:
    return build_arg_parser().parse_args()

def _normalize_ticker_list(tickers: list[str]) -> list[str]:
    seen = set()
    out = []
    for t in tickers:
        x = str(t).strip().upper()
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out

def resolve_ticker_universe(all_tickers: list[str], args: argparse.Namespace) -> tuple[str, list[str]]:
    base_universe = _normalize_ticker_list(all_tickers)
    if args.tickers:
        override = _normalize_ticker_list(args.tickers.split(','))
        if not override:
            raise RuntimeError('Ticker override provided but no valid tickers found.')
        base_universe = override
    return ('full_universe', base_universe) if args.full_universe else ('smoke', base_universe[:DEFAULT_SMOKE_TICKER_COUNT])

def generate_run_id(run_mode: str) -> str:
    return f"{SCRIPT_VERSION}_{run_mode}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"

def run_schema_check(output_dir: str) -> dict[str, Any]:
    duplicate_columns = len(OUTPUT_COLUMNS) != len(set(OUTPUT_COLUMNS))
    exact_count = len(OUTPUT_COLUMNS)
    os.makedirs(output_dir, exist_ok=True)
    with open(_schema_check_path(output_dir), 'w', encoding='utf-8') as f:
        f.write(f"script_version={SCRIPT_VERSION}\ncolumn_count={exact_count}\nhas_duplicates={duplicate_columns}\nstatus={('PASS' if not duplicate_columns else 'FAIL')}\n")
    return {'script_version': SCRIPT_VERSION, 'column_count': exact_count, 'has_duplicates': duplicate_columns, 'status': 'PASS' if not duplicate_columns else 'FAIL'}

def ensure_schema_is_locked(output_dir: str) -> None:
    if run_schema_check(output_dir)['status'] != 'PASS':
        raise RuntimeError('Schema check failed.')

def provider_preflight_summary() -> dict[str, Any]:
    return {'script_version': SCRIPT_VERSION, 'alpaca_configured': bool(ALPACA_API_KEY and 'YOUR_' not in ALPACA_API_KEY and ALPACA_SECRET_KEY and ('YOUR_' not in ALPACA_SECRET_KEY)), 'fmp_configured': bool(FMP_API_KEY and 'YOUR_' not in FMP_API_KEY), 'finnhub_configured': bool(FINNHUB_API_KEY and 'YOUR_' not in FINNHUB_API_KEY), 'polygon_configured': bool(POLYGON_API_KEY and 'YOUR_' not in POLYGON_API_KEY), 'fred_configured': bool(FRED_API_KEY and 'YOUR_' not in FRED_API_KEY), 'sec_user_agent_configured': bool(SEC_USER_AGENT and '@' in SEC_USER_AGENT)}

def write_provider_summary(output_dir: str) -> dict[str, Any]:
    summary = provider_preflight_summary()
    with open(_provider_summary_path(output_dir), 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    return summary

def _read_csv_rows(csv_path: str) -> tuple[list[str], list[dict[str, Any]]]:
    if not os.path.exists(csv_path):
        return ([], [])
    with open(csv_path, 'r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        header = reader.fieldnames or []
    return (header, rows)

def validate_smoke_outputs(output_dir: str, selected_tickers: list[str], checkpoint: dict[str, Any], run_id: str) -> dict[str, Any]:
    extractor_path = _extractor_all_path(output_dir)
    run_manifest_path = _manifest_path(output_dir)
    provider_summary_path = _provider_summary_path(output_dir)
    schema_check_path = _schema_check_path(output_dir)
    checkpoint_path = _checkpoint_path(output_dir)
    with open(run_manifest_path, 'w', encoding='utf-8') as f:
        json.dump({'script_version': SCRIPT_VERSION, 'run_id': run_id, 'run_mode': 'smoke'}, f)
    header, rows = _read_csv_rows(extractor_path)
    completed_rows = checkpoint_completed_rows_in_universe_order(checkpoint, selected_tickers)
    completed_count = len(completed_rows)
    duplicate_tickers = len({str(r.get('ticker', '')).strip().upper() for r in rows}) != len(rows)
    row_versions_ok = all((str(r.get('script_version')) == SCRIPT_VERSION for r in rows)) if rows else False
    row_run_ids_ok = all((str(r.get('run_id')) == run_id for r in rows)) if rows else False
    null_fields_ok = all(('null_fields' in r and r.get('null_fields') is not None for r in rows)) if rows else False
    criteria_results = []

    def add_result(criterion: str, passed: bool, detail: str) -> None:
        criteria_results.append({'criterion': criterion, 'passed': bool(passed), 'detail': detail})
    add_result('output_columns_exact_match', len(OUTPUT_COLUMNS) == len(set(OUTPUT_COLUMNS)), f'column_count={len(OUTPUT_COLUMNS)}')
    add_result('smoke_mode_uses_3_tickers_by_default', len(selected_tickers) <= DEFAULT_SMOKE_TICKER_COUNT, f'selected_ticker_count={len(selected_tickers)}')
    required_outputs_exist = all((os.path.exists(p) for p in [extractor_path, run_manifest_path, provider_summary_path, schema_check_path, checkpoint_path]))
    add_result('required_output_files_exist', required_outputs_exist, f'extractor={os.path.exists(extractor_path)} manifest={os.path.exists(run_manifest_path)} provider_summary={os.path.exists(provider_summary_path)} schema_check={os.path.exists(schema_check_path)} checkpoint={os.path.exists(checkpoint_path)}')
    add_result('extractor_csv_has_locked_header', header == OUTPUT_COLUMNS, f'header_matches={header == OUTPUT_COLUMNS}')
    add_result('rows_written_match_completed_tickers', len(rows) == completed_count, f'csv_rows={len(rows)} completed_rows={completed_count}')
    add_result('every_row_has_script_version', row_versions_ok, f'row_versions_ok={row_versions_ok}')
    add_result('every_row_has_run_id', row_run_ids_ok, f'row_run_ids_ok={row_run_ids_ok}')
    add_result('every_row_has_null_fields_column', null_fields_ok, f'null_fields_ok={null_fields_ok}')
    add_result('no_duplicate_tickers_in_output', not duplicate_tickers, f'duplicate_tickers={duplicate_tickers}')
    add_result('checkpoint_write_through_active', os.path.exists(checkpoint_path) and completed_count >= 1, f'checkpoint_exists={os.path.exists(checkpoint_path)} completed_count={completed_count}')
    overall_pass = all((item['passed'] for item in criteria_results))
    manifest = {'script_version': SCRIPT_VERSION, 'run_id': run_id, 'run_mode': 'smoke', 'created_at_utc': _utc_now_iso_manifest(), 'smoke_test_passed': overall_pass, 'selected_tickers': selected_tickers, 'criteria_results': criteria_results, 'summary': {'csv_row_count': len(rows), 'completed_ticker_count': completed_count, 'duplicate_tickers': duplicate_tickers}}
    with open(run_manifest_path, 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    return manifest

def load_previous_smoke_manifest(output_dir: str) -> Optional[dict[str, Any]]:
    path = _manifest_path(output_dir)
    if not os.path.exists(path):
        return None
    with open(path, 'r', encoding='utf-8') as f:
        payload = json.load(f)
    return payload if payload.get('run_mode') == 'smoke' else None

def require_passing_smoke_before_full_run(output_dir: str, allow_override: bool=False) -> None:
    if allow_override:
        return
    manifest = load_previous_smoke_manifest(output_dir)
    if manifest is None:
        raise RuntimeError('Full-universe run blocked. No smoke manifest found.')
    if manifest.get('script_version') != SCRIPT_VERSION:
        raise RuntimeError('Full-universe run blocked. Smoke manifest version mismatch.')
    if manifest.get('smoke_test_passed') is not True:
        raise RuntimeError('Full-universe run blocked. Previous smoke test did not pass.')

def write_output_csvs_from_checkpoint(output_dir: str, checkpoint: dict[str, Any], selected_tickers: list[str]) -> None:
    os.makedirs(output_dir, exist_ok=True)
    rows = checkpoint_completed_rows_in_universe_order(checkpoint, selected_tickers)
    survivor_rows = [r for r in rows if str(r.get('hard_floor_result')) == 'PASS' and str(r.get('stacking_result')) == 'PASS' and (str(r.get('ccg_result')) == 'PASS')]
    eliminated_rows = [r for r in rows if 'FAIL' in {str(r.get('hard_floor_result')), str(r.get('stacking_result')), str(r.get('ccg_result'))}]
    incomplete_rows = [r for r in rows if r not in survivor_rows and r not in eliminated_rows]

    def write_csv(path: str, rowset: list[dict[str, Any]]) -> None:
        with open(path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS, extrasaction='ignore')
            writer.writeheader()
            for row in rowset:
                writer.writerow({col: row.get(col) for col in OUTPUT_COLUMNS})
    write_csv(_extractor_all_path(output_dir), rows)
    write_csv(_survivors_path(output_dir), survivor_rows)
    write_csv(_eliminated_path(output_dir), eliminated_rows)
    write_csv(_incomplete_path(output_dir), incomplete_rows)

def prepare_run(all_tickers: list[str]) -> tuple[argparse.Namespace, str, str, list[str], dict[str, Any]]:
    args = parse_cli_args()
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)
    ensure_schema_is_locked(output_dir)
    write_provider_summary(output_dir)
    run_mode, selected_tickers = resolve_ticker_universe(all_tickers, args)
    if run_mode == 'full_universe':
        require_passing_smoke_before_full_run(output_dir, bool(args.allow_full_without_smoke))
    cp = load_or_init_checkpoint(output_dir, generate_run_id(run_mode), run_mode, selected_tickers, bool(args.force))
    return (args, output_dir, run_mode, selected_tickers, cp)

def finalize_run(output_dir: str, run_mode: str, selected_tickers: list[str], checkpoint: dict[str, Any]) -> dict[str, Any]:
    write_output_csvs_from_checkpoint(output_dir, checkpoint, selected_tickers)
    if run_mode == 'smoke':
        return validate_smoke_outputs(output_dir, selected_tickers, checkpoint, checkpoint['run_id'])
    manifest = {'script_version': SCRIPT_VERSION, 'run_id': checkpoint['run_id'], 'run_mode': run_mode, 'created_at_utc': _utc_now_iso_manifest(), 'selected_tickers': selected_tickers, 'checkpoint_summary': checkpoint_summary(checkpoint), 'smoke_test_passed': None}
    with open(_manifest_path(output_dir), 'w', encoding='utf-8') as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)
    return manifest

def _extract_cik_from_fmp(profile_payload: Any, core_payload: Any) -> Optional[str]:
    if isinstance(core_payload, dict) and core_payload.get('cik'):
        return str(core_payload.get('cik'))
    if isinstance(core_payload, list) and core_payload and core_payload[0].get('cik'):
        return str(core_payload[0].get('cik'))
    if isinstance(profile_payload, list) and profile_payload and profile_payload[0].get('cik'):
        return str(profile_payload[0].get('cik'))
    return None

def _latest_buy_identity_from_transactions(raw_finnhub_insider_transactions: Any) -> tuple[Optional[str], Optional[str]]:
    data = _get_nested(raw_finnhub_insider_transactions, 'data')
    rows = data if isinstance(data, list) else []
    if not rows:
        return (None, None)
    def _row_sort_key(r: dict) -> str:
        return _parse_iso_date(_latest_non_null([r.get('filingDate'), r.get('transactionDate')])) or '0000-00-00'
    rows = sorted(rows, key=_row_sort_key, reverse=True)
    for r in rows:
        code = str(_latest_non_null([r.get('transactionCode'), r.get('transactionType')]) or '').upper()
        if code in {'P', 'BUY'}:
            return (
                _parse_iso_date(_latest_non_null([r.get('filingDate'), r.get('transactionDate')])),
                str(r.get('name') or r.get('personName') or '').strip() or None,
            )
    return (None, None)

def _latest_buy_filing_date_from_transactions(raw_finnhub_insider_transactions: Any) -> Optional[str]:
    filing_date, _buyer_name = _latest_buy_identity_from_transactions(raw_finnhub_insider_transactions)
    return filing_date

def _latest_form4_url_from_submissions(sec_cik: Optional[str], raw_sec_submissions: Any, target_filing_date: Optional[str]=None, target_buyer_name: Optional[str]=None) -> Optional[str]:
    if not sec_cik:
        return None
    recent = _get_nested(raw_sec_submissions, 'filings', 'recent') or {}
    forms = recent.get('form')
    accessions = recent.get('accessionNumber')
    primary_docs = recent.get('primaryDocument')
    filing_dates = recent.get('filingDate')
    reporting_owners = recent.get('reportingOwner')

    if not isinstance(forms, list):
        return None

    form4_idxs = []
    buyer_norm = str(target_buyer_name or '').strip().lower() or None
    for i, form in enumerate(forms):
        if str(form).upper() != '4':
            continue
        fd = filing_dates[i] if isinstance(filing_dates, list) and i < len(filing_dates) else None
        fd = _parse_iso_date(fd)
        if target_filing_date is not None and fd != target_filing_date:
            continue
        if buyer_norm and isinstance(reporting_owners, list) and i < len(reporting_owners):
            owner = str(reporting_owners[i] or '').strip().lower()
            if owner:
                owner_tokens = {tok for tok in re.split(r'[^a-z0-9]+', owner) if tok}
                buyer_tokens = {tok for tok in re.split(r'[^a-z0-9]+', buyer_norm) if tok}
                if owner_tokens and buyer_tokens and owner_tokens.isdisjoint(buyer_tokens):
                    continue
        form4_idxs.append(i)

    if not form4_idxs:
        return None

    def _form4_idx_sort_key(idx: int) -> str:
        if isinstance(filing_dates, list) and idx < len(filing_dates):
            return str(_parse_iso_date(filing_dates[idx]) or '0000-00-00')
        return '0000-00-00'

    form4_idxs = sorted(form4_idxs, key=_form4_idx_sort_key, reverse=True)
    i = form4_idxs[0]

    accession = accessions[i] if isinstance(accessions, list) and i < len(accessions) else None
    primary_doc = primary_docs[i] if isinstance(primary_docs, list) and i < len(primary_docs) else None
    if accession and primary_doc:
        try:
            _cik_int = str(int(str(sec_cik).strip()))
        except (ValueError, TypeError):
            _cik_int = str(sec_cik).strip().lstrip('0') or str(sec_cik).strip()
        return f"https://www.sec.gov/Archives/edgar/data/{_cik_int}/{str(accession).replace('-', '')}/{primary_doc}"
    return None

def _fred_10y_30d_change_bps() -> Optional[float]:
    try:
        payload = fred_get_series_observations('DGS10')
        obs = payload.get('observations', []) if isinstance(payload, dict) else []
        vals = []
        for o in obs[-90:]:
            try:
                vals.append(float(o.get('value')))
            except Exception:
                pass
        if len(vals) < 31:
            return None
        return (vals[-1] - vals[-31]) * 100.0
    except Exception:
        return None

def _start_end_for_daily_bars() -> tuple[str, str]:
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=800)
    return (start.isoformat(), end.isoformat())

def _safe_call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception:
        return None

def _group_values(rows: list[dict[str, Any]], group_key: str, value_key: str) -> dict[str, list[float]]:
    out: dict[str, list[float]] = {}
    for row in rows:
        g = row.get(group_key)
        v = row.get(value_key)
        if g is None or v is None:
            continue
        try:
            out.setdefault(str(g), []).append(float(v))
        except Exception:
            pass
    return out

def _quintile_rank(values: list[float], value: float) -> int:
    if not values:
        return 3
    vals = sorted(values)
    pct = sum((1 for x in vals if x <= value)) / len(vals)
    if pct <= 0.2:
        return 1
    if pct <= 0.4:
        return 2
    if pct <= 0.6:
        return 3
    if pct <= 0.8:
        return 4
    return 5

def _sector_medians(rows: list[dict[str, Any]], sector: str, key: str) -> Optional[float]:
    vals = []
    for row in rows:
        if row.get('sector') == sector and row.get(key) is not None:
            try:
                vals.append(float(row.get(key)))
            except Exception:
                pass
    return statistics.median(vals) if vals else None

def _merge_row(row: dict[str, Any], section: dict[str, Any]) -> None:
    for k, v in section.items():
        row[k] = v
CURRENT_SOFT_CRASH_FLAG_ACTIVE = True
CURRENT_FULL_CRASH_FLAG_ACTIVE = False
CURRENT_MTUM_VTV_CAP_ACTIVE = True
CURRENT_HY_OAS_BEAR_RALLY_ACTIVE = False  # documented limitation / external regime input

def fmp_get_cash_flow_ttm(ticker: str) -> Any:
    return _http_get_json(f'{FMP_BASE}/api/v3/cash-flow-statement/{ticker}', params={'period': 'quarter', 'limit': 4, 'apikey': FMP_API_KEY})

def fmp_get_income_statement_ttm(ticker: str) -> Any:
    return _http_get_json(f'{FMP_BASE}/api/v3/income-statement/{ticker}', params={'period': 'quarter', 'limit': 4, 'apikey': FMP_API_KEY})

def parse_identity_section(ticker: str, list_source: str, raw_fmp_profile: Any, raw_massive_details: Any) -> dict[str, Any]:
    fields = ['ticker', 'list_source', 'company_name', 'exchange', 'sector', 'industry', 'gics_4digit', 'market_cap', 'market_cap_tier', 'adr_flag', 'chinese_adr_flag', 'dual_listing_flag', 'saas_high_growth_flag', 'financial_reit_utility_asset_light_flag', 'sector_classification_rule', 'altman_model_variant', 'solvency_hard_floor_rule_used']
    out = _none_dict(fields)
    fmp = _first_list_item(raw_fmp_profile)
    massive = _get_nested(raw_massive_details, 'results') or {}
    t = str(ticker).strip().upper()
    out['ticker'] = ticker
    out['list_source'] = list_source
    out['company_name'] = _latest_non_null([fmp.get('companyName'), massive.get('name')])
    out['exchange'] = _latest_non_null([fmp.get('exchangeShortName'), massive.get('primary_exchange')])
    out['sector'] = fmp.get('sector') or None
    out['industry'] = fmp.get('industry') or None
    # sic_description is NOT a sector/industry substitute — it is a regulatory classification code description
    out['market_cap'] = _latest_non_null([_to_float(massive.get('market_cap')), _to_float(fmp.get('mktCap'))])
    mc = out['market_cap']
    if mc is not None:
        out['market_cap_tier'] = 'Large' if mc >= 750000000 else 'Mid' if mc >= 200000000 else 'Small' if mc >= 50000000 else 'Micro'
    adr_manual = MANUAL_ADR_FLAG_BY_TICKER.get(t)
    _provider_is_adr = _latest_non_null([_to_int(fmp.get('isAdr')), _get_nested(raw_massive_details, 'results', 'is_adr')])
    if adr_manual is not None:
        out['adr_flag'] = bool(adr_manual)
    elif _provider_is_adr in (1, True):
        out['adr_flag'] = True
    elif _provider_is_adr in (0, False):
        out['adr_flag'] = False
    else:
        out['adr_flag'] = None
    china_manual = MANUAL_CHINESE_ADR_FLAG_BY_TICKER.get(t)
    out['chinese_adr_flag'] = bool(china_manual) if china_manual is not None else None  # manual only — provider ADR metadata insufficient for Chinese issuer determination
    out['dual_listing_flag'] = MANUAL_DUAL_LISTING_FLAG_BY_TICKER.get(t)
    out['gics_4digit'] = MANUAL_GICS_4DIGIT_BY_TICKER.get(t)
    spac_manual = MANUAL_SPAC_FLAG_BY_TICKER.get(t)
    out['_spac_flag'] = bool(spac_manual) if spac_manual is not None else None  # manual/provider only — name-matching heuristic removed
    sector = (out['sector'] or '').lower()
    industry = (out['industry'] or '').lower()
    _CHS_SECTORS = {'financial', 'financials', 'financial services', 'banking', 'insurance', 'reit', 'real estate', 'utilities', 'utility'}
    _CHS_INDUSTRIES = {'bank', 'banks', 'insurance', 'reit', 'asset management', 'capital markets', 'thrifts', 'mortgage', 'diversified financial', 'software', 'internet', 'saas', 'application software', 'systems software', 'it services', 'interactive media'}
    _MANUFACTURING_SECTORS = {'industrials', 'materials', 'basic materials', 'energy'}
    _MANUFACTURING_INDUSTRIES = {'manufacturing', 'oil & gas', 'mining', 'chemicals', 'steel', 'aerospace', 'defense', 'construction', 'machinery', 'auto', 'paper', 'packaging', 'semiconductor', 'hardware'}

    def _matches(text_: str, terms: set) -> bool:
        return any(t_ in text_ for t_ in terms)

    if _matches(sector, _CHS_SECTORS) or _matches(industry, _CHS_INDUSTRIES):
        if 'real estate' in sector or 'reit' in sector or 'reit' in industry:
            out['financial_reit_utility_asset_light_flag'] = 'reit'
        elif 'utility' in sector or 'utilities' in sector:
            out['financial_reit_utility_asset_light_flag'] = 'utility'
        elif any(t_ in industry for t_ in ['software', 'internet', 'saas', 'application software', 'systems software', 'it services', 'interactive media']):
            out['financial_reit_utility_asset_light_flag'] = 'asset_light'
        else:
            out['financial_reit_utility_asset_light_flag'] = 'financial'
        out['altman_model_variant'] = 'chs_only'
    elif _matches(sector, _MANUFACTURING_SECTORS) or _matches(industry, _MANUFACTURING_INDUSTRIES):
        out['financial_reit_utility_asset_light_flag'] = 'other'
        out['altman_model_variant'] = '1968_original'
    else:
        out['financial_reit_utility_asset_light_flag'] = 'other'
        out['altman_model_variant'] = '1995_z_double_prime'
    _high_growth_terms = ['software', 'saas', 'internet', 'application software', 'systems software', 'interactive media', 'it services']
    if industry:
        out['saas_high_growth_flag'] = True if any(x in industry for x in _high_growth_terms) else False
    else:
        out['saas_high_growth_flag'] = None
    out['sector_classification_rule'] = 'fmp_sector_industry_with_manual_gics_dual_listing_overrides'
    variant = out['altman_model_variant']
    if variant == '1968_original':
        out['solvency_hard_floor_rule_used'] = 'altman_1968_plus_piotroski'
    elif variant == '1995_z_double_prime':
        out['solvency_hard_floor_rule_used'] = 'altman_z_double_prime_plus_piotroski'
    else:
        out['solvency_hard_floor_rule_used'] = 'chs_required_manual'
    return out

def _extract_10b5_1_adoption_date(text: str) -> Optional[str]:
    patterns = ['10b5-1[^0-9]{0,40}(\\d{4}-\\d{2}-\\d{2})', '10b5-1[^0-9]{0,40}(\\d{1,2}/\\d{1,2}/\\d{4})', '10b5-1[^A-Za-z]{0,20}([A-Za-z]+\\s+\\d{1,2},\\s*\\d{4})']
    for pat in patterns:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if not m:
            continue
        raw = m.group(1)
        for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%B %d, %Y', '%b %d, %Y'):
            try:
                return datetime.strptime(raw, fmt).date().isoformat()
            except Exception:
                pass
    return None

def _approx_abs_trading_days_diff(date_a_str: Optional[str], date_b_str: Optional[str]) -> Optional[int]:
    if date_a_str is None or date_b_str is None:
        return None
    try:
        d0 = datetime.strptime(date_a_str, '%Y-%m-%d').date()
        d1 = datetime.strptime(date_b_str, '%Y-%m-%d').date()
        return int(round(abs((d1 - d0).days) * 5.0 / 7.0))
    except Exception:
        return None

def _sum_latest_quarters(rows: Any, field: str, limit: int=4) -> Optional[float]:
    if not isinstance(rows, list):
        return None
    vals = []
    for row in rows[:limit]:
        v = _to_float(row.get(field))
        if v is not None:
            vals.append(v)
    if len(vals) < min(limit, len(rows[:limit])) or not vals:
        return None
    return float(sum(vals))

def _extract_last_earnings_date_from_payloads(raw_finnhub_earnings_surprises: Any, raw_fmp_earnings_surprises: Any, raw_fmp_earnings_report: Any, raw_finnhub_earnings_calendar: Any) -> Optional[str]:
    finnhub_earn = raw_finnhub_earnings_surprises if isinstance(raw_finnhub_earnings_surprises, list) else []
    fmp_earn = raw_fmp_earnings_surprises if isinstance(raw_fmp_earnings_surprises, list) else []
    fmp_report = raw_fmp_earnings_report if isinstance(raw_fmp_earnings_report, list) else []

    def _earn_row_date(r: dict) -> Optional[str]:
        return _parse_iso_date(_latest_non_null([
            r.get('date'),
            r.get('calendarDate'),
            r.get('reportedDate'),
        ]))

    def _earn_row_sort_key(r: dict) -> str:
        return str(_earn_row_date(r) or '0000-00-00')

    combined_earn = []
    if finnhub_earn:
        combined_earn.extend(finnhub_earn)
    if fmp_earn:
        combined_earn.extend(fmp_earn)
    if fmp_report:
        combined_earn.extend(fmp_report)
    if combined_earn:
        _today_str = date.today().isoformat()
        combined_earn = [r for r in combined_earn if (_earn_row_date(r) or '9999-12-31') <= _today_str]
        combined_earn = sorted(combined_earn, key=_earn_row_sort_key, reverse=True)

    latest = combined_earn[0] if combined_earn else None
    if isinstance(latest, dict):
        parsed = _earn_row_date(latest)
        if parsed:
            return parsed

    if isinstance(raw_finnhub_earnings_calendar, dict):
        earnings_list = raw_finnhub_earnings_calendar.get('earningsCalendar') or raw_finnhub_earnings_calendar.get('earningsCalendarList')
        if isinstance(earnings_list, list):
            _today = date.today()
            _past_dates = []
            for _item in earnings_list:
                _d = _parse_iso_date(_item.get('date'))
                if _d is not None:
                    try:
                        _dd = datetime.strptime(_d, '%Y-%m-%d').date()
                        if _dd <= _today:
                            _past_dates.append(_d)
                    except Exception:
                        pass
            return max(_past_dates) if _past_dates else None
    return None

def _compute_3d_post_earnings_return_from_bars(bars: list[dict[str, Any]], earnings_date_str: Optional[str]) -> Optional[float]:
    if not bars or earnings_date_str is None:
        return None
    series = []
    for b in bars:
        d = _extract_bar_date(b)
        c = _extract_close_from_bar(b)
        if d is not None and c is not None:
            series.append((d, c))
    if not series:
        return None
    series.sort(key=lambda x: x[0])
    idx = None
    for i, (d, _) in enumerate(series):
        if d >= earnings_date_str:
            idx = i
            break
    if idx is None or idx == 0 or idx + 1 >= len(series):
        return None
    start_close = series[idx - 1][1]
    end_close = series[idx + 1][1]
    if start_close in (None, 0) or end_close is None:
        return None
    return end_close / start_close - 1.0

def _compute_trailing_return_from_bars(bars: list[dict[str, Any]], trading_days: int) -> Optional[float]:
    if not bars:
        return None
    vals = [_extract_close_from_bar(b) for b in bars if _extract_close_from_bar(b) is not None]
    if len(vals) <= trading_days:
        return None
    start = vals[-(trading_days + 1)]
    end = vals[-1]
    if start in (None, 0) or end is None:
        return None
    return end / start - 1.0

def _compute_trailing_return_from_bars_anchored(bars: list[dict[str, Any]], trading_days: int, anchor_date_str: Optional[str]=None) -> Optional[float]:
    if not bars:
        return None
    series: list[tuple[str, float]] = []
    for b in bars:
        d = _extract_bar_date(b)
        c = _extract_close_from_bar(b)
        if d is not None and c is not None:
            series.append((d, c))
    if not series:
        return None
    series.sort(key=lambda x: x[0])
    if anchor_date_str is None:
        idx = len(series) - 1
    else:
        idx = None
        for i, (d, _) in enumerate(series):
            if d <= anchor_date_str:
                idx = i
            else:
                break
        if idx is None:
            return None
    if idx - trading_days < 0:
        return None
    start = series[idx - trading_days][1]
    end = series[idx][1]
    if start in (None, 0) or end is None:
        return None
    return end / start - 1.0

def parse_profitability_section(raw_fmp_key_metrics_ttm: Any, raw_fmp_ratios_ttm: Any, raw_fmp_key_metrics_q: Any, raw_fmp_dcf: Any, raw_fmp_cash_flow_annual: Any, raw_fmp_income_ttm: Any, raw_fmp_balance_annual: Any, raw_fmp_balance_quarter: Any=None, raw_fmp_cash_flow_ttm: Any=None) -> dict[str, Any]:
    fields = ['roic', 'roic_source', 'wacc', 'roic_minus_wacc', 'roic_trend_2q', 'fcf_ni_ratio', 'sbc_pct_revenue', 'economic_roic_used', 'roe', 'nim_trend', 'fre_growth_pct', 'fcf_fy0', 'fcf_fy1', 'fcf_fy2', 'fcf_fy3', 'free_cash_flow_negative_3y_streak']
    out = _none_dict(fields)
    km_ttm = _first_list_item(raw_fmp_key_metrics_ttm)
    ratios_ttm = _first_list_item(raw_fmp_ratios_ttm)
    dcf = _first_list_item(raw_fmp_dcf)
    income_rows = raw_fmp_income_ttm if isinstance(raw_fmp_income_ttm, list) else []
    cf_rows_q = raw_fmp_cash_flow_ttm if isinstance(raw_fmp_cash_flow_ttm, list) else []
    q_rows = raw_fmp_key_metrics_q if isinstance(raw_fmp_key_metrics_q, list) else []
    bal_rows_a = raw_fmp_balance_annual if isinstance(raw_fmp_balance_annual, list) else []
    bal_rows_q = raw_fmp_balance_quarter if isinstance(raw_fmp_balance_quarter, list) else []

    def _stmt_row_sort_key(r: dict) -> str:
        return str(_latest_non_null([
            r.get('date'),
            r.get('fillingDate'),
            r.get('acceptedDate'),
            r.get('calendarYear'),
            r.get('period'),
        ]) or '0000-00-00')

    income_rows = sorted(income_rows, key=_stmt_row_sort_key, reverse=True)
    cf_rows_q = sorted(cf_rows_q, key=_stmt_row_sort_key, reverse=True)
    q_rows = sorted(q_rows, key=_stmt_row_sort_key, reverse=True)
    bal_rows_a = sorted(bal_rows_a, key=_stmt_row_sort_key, reverse=True)
    bal_rows_q = sorted(bal_rows_q, key=_stmt_row_sort_key, reverse=True)

    _ticker_sym = str(_latest_non_null([km_ttm.get('symbol'), ratios_ttm.get('symbol')]) or '').strip().upper() or None
    _roic_manual = MANUAL_TIKR_ROIC_BY_TICKER.get(_ticker_sym) if _ticker_sym else None
    _economic_roic_manual = MANUAL_TIKR_ECONOMIC_ROIC_BY_TICKER.get(_ticker_sym) if _ticker_sym else None
    _roic_km = _latest_non_null([_to_float(km_ttm.get('roic')), _to_float(km_ttm.get('returnOnInvestedCapital'))])
    _roic_ratios = _to_float(ratios_ttm.get('returnOnInvestedCapital'))
    if _roic_manual is not None:
        out['roic'] = _to_float(_roic_manual)
        out['roic_source'] = 'tikr_manual'
    elif _roic_km is not None:
        out['roic'] = _roic_km
        out['roic_source'] = 'fmp_key_metrics'
    elif _roic_ratios is not None:
        out['roic'] = _roic_ratios
        out['roic_source'] = 'fmp_ratios'
    else:
        ebit = _sum_latest_quarters(income_rows, 'ebit', 4)
        if ebit is None:
            ebit = _sum_latest_quarters(income_rows, 'operatingIncome', 4)
        etr = _latest_non_null([_to_float(ratios_ttm.get('effectiveTaxRate')), _to_float(dcf.get('taxRate'))])
        bal_latest = bal_rows_q[0] if bal_rows_q else (bal_rows_a[0] if bal_rows_a else {})
        equity = _to_float(_latest_non_null([bal_latest.get('totalStockholdersEquity'), bal_latest.get('totalEquity')]))
        debt = _to_float(bal_latest.get('totalDebt'))
        cash = _latest_non_null([_to_float(bal_latest.get('cashAndCashEquivalents')), _to_float(bal_latest.get('cashAndShortTermInvestments'))])
        invested_capital = None if equity is None or debt is None or cash is None else equity + debt - cash
        nopat = None if ebit is None else ebit * (1.0 - (etr if etr is not None else 0.21))
        out['roic'] = _safe_div(nopat, invested_capital)
        out['roic_source'] = 'nopat_fallback' if out['roic'] is not None else None
    out['wacc'] = _latest_non_null([_to_float(dcf.get('wacc')), _to_float(dcf.get('weightedAverageCostOfCapital'))])
    if out['roic'] is not None and out['wacc'] is not None:
        out['roic_minus_wacc'] = out['roic'] - out['wacc']
    out['roe'] = _latest_non_null([_to_float(ratios_ttm.get('returnOnEquity')), _to_float(km_ttm.get('roe'))])
    roic_q = []
    for row in q_rows[:3]:
        roic_q.append(_latest_non_null([_to_float(row.get('roic')), _to_float(row.get('returnOnInvestedCapital'))]))
    if len([x for x in roic_q if x is not None]) >= 3:
        q1, q2, q3 = roic_q[0], roic_q[1], roic_q[2]
        if q3 < q2 < q1:
            out['roic_trend_2q'] = 'improving'
        elif q3 > q2 > q1:
            out['roic_trend_2q'] = 'deteriorating'
        else:
            out['roic_trend_2q'] = 'flat'
    else:
        out['roic_trend_2q'] = None
    cf_rows = raw_fmp_cash_flow_annual if isinstance(raw_fmp_cash_flow_annual, list) else []
    cf_rows = sorted(cf_rows, key=_stmt_row_sort_key, reverse=True)
    fcf_values = []
    for row in cf_rows[:4]:
        fcf_values.append(_to_float(row.get('freeCashFlow')))
    while len(fcf_values) < 4:
        fcf_values.append(None)
    out['fcf_fy0'], out['fcf_fy1'], out['fcf_fy2'], out['fcf_fy3'] = fcf_values[:4]
    streak_vals = [out['fcf_fy0'], out['fcf_fy1'], out['fcf_fy2']]
    out['free_cash_flow_negative_3y_streak'] = None if any((v is None for v in streak_vals)) else all((v < 0 for v in streak_vals))
    fcf_ttm = _sum_latest_quarters(cf_rows_q, 'freeCashFlow', 4)
    net_income_ttm = _sum_latest_quarters(income_rows, 'netIncome', 4)
    out['fcf_ni_ratio'] = _safe_div(fcf_ttm, net_income_ttm)
    sbc = _sum_latest_quarters(cf_rows_q, 'stockBasedCompensation', 4)
    revenue_ttm = _sum_latest_quarters(income_rows, 'revenue', 4)
    out['sbc_pct_revenue'] = None if sbc is None or revenue_ttm in (None, 0) else sbc / revenue_ttm * 100.0
    if out['sbc_pct_revenue'] is not None and out['sbc_pct_revenue'] > 5.0:
        economic_roic = _latest_non_null([
            _to_float(_economic_roic_manual) if _economic_roic_manual is not None else None,
            _to_float(km_ttm.get('economicROIC')),
            _to_float(ratios_ttm.get('economicROIC')),
        ])
        if economic_roic is not None:
            out['roic'] = economic_roic
            out['roic_source'] = 'economic_roic'
            out['economic_roic_used'] = True
            if out['wacc'] is not None:
                out['roic_minus_wacc'] = out['roic'] - out['wacc']
        else:
            out['economic_roic_used'] = False
    else:
        out['economic_roic_used'] = None if out['sbc_pct_revenue'] is None else False
    t = str(_first_list_item(raw_fmp_key_metrics_ttm).get('symbol') or '').strip().upper() if isinstance(_first_list_item(raw_fmp_key_metrics_ttm), dict) else None
    if not t and isinstance(_first_list_item(raw_fmp_ratios_ttm), dict):
        t = str(_first_list_item(raw_fmp_ratios_ttm).get('symbol') or '').strip().upper() or None
    out['nim_trend'] = MANUAL_NIM_TREND_BY_TICKER.get(t) if t else None
    out['fre_growth_pct'] = MANUAL_FRE_GROWTH_PCT_BY_TICKER.get(t) if t else None
    return out

def parse_solvency_section(raw_fmp_financial_scores: Any, altman_model_variant: Optional[str], days_to_cover: Optional[float], ticker: Optional[str]=None) -> dict[str, Any]:
    fields = ['altman_z_score', 'piotroski_score', 'chs_result', 'altman_piotroski_conflict_flag', 'dtc_7_15_modifier_flag', 'dtc_independence_check']
    out = _none_dict(fields)
    t = str(ticker or '').strip().upper() if ticker else ''
    score = _first_list_item(raw_fmp_financial_scores)
    _standard_altman = _latest_non_null([_to_float(score.get('altmanZScore')), _to_float(score.get('altmanZ'))])
    out['altman_z_score'] = None if altman_model_variant == '1995_z_double_prime' else _standard_altman
    out['piotroski_score'] = _latest_non_null([_to_float(score.get('piotroskiScore')), _to_float(score.get('piotroski'))])
    out['chs_result'] = MANUAL_CHS_RESULT_BY_TICKER.get(t) if t else None

    def alt_band(a: Optional[float], variant: Optional[str]) -> Optional[int]:
        if a is None:
            return None
        a = float(a)
        if variant == '1995_z_double_prime':
            if a > 2.6:
                return 10
            if 1.1 <= a <= 2.6:
                return 5
            return 2
        if a > 2.99:
            return 10
        if 2.67 <= a <= 2.99:
            return 8
        if 2.0 <= a < 2.67:
            return 5
        return 2

    def piot_band(p: Optional[float]) -> Optional[int]:
        if p is None:
            return None
        p = float(p)
        if p >= 8:
            return 10
        if p == 7:
            return 8
        if p in (5.0, 6.0):
            return 5
        if p == 4.0:
            return 2
        return 0
    a_band = alt_band(out['altman_z_score'], altman_model_variant)
    p_band = piot_band(out['piotroski_score'])
    if a_band is not None and p_band is not None and (p_band != 0):
        out['altman_piotroski_conflict_flag'] = a_band != p_band
    out['dtc_7_15_modifier_flag'] = None if days_to_cover is None else 7 <= days_to_cover <= 15
    out['dtc_independence_check'] = MANUAL_DTC_INDEPENDENCE_BY_TICKER.get(t) if t else None
    return out

def _build_revision_source_from_fmp_analyst_estimates(est_rows: Any, ticker: Optional[str]=None) -> Optional[dict[str, int]]:
    """
    Do not infer revision momentum from adjacent estimate rows.
    v6.8 requires point-in-time same-period revision history.
    If that source is unavailable, return None and let the modifier be skipped.
    A manual source may be supplied only if explicitly populated in the approved workflow.
    """
    t = str(ticker or '').strip().upper() if ticker else ''
    manual = MANUAL_REVISION_SOURCE_BY_TICKER.get(t) if t else None
    return manual if isinstance(manual, dict) else None

def score_factor_3(row: dict[str, Any]) -> dict[str, Any]:
    roic = row.get('roic')
    roic_minus_wacc = row.get('roic_minus_wacc')
    roic_trend = row.get('roic_trend_2q')
    wacc = row.get('wacc')
    fin_sub_score = row.get('_financial_substitution_score')
    roic_bucket = row.get('_roic_vs_sector_bucket')
    raw = None
    adjusted = None
    if fin_sub_score is not None:
        raw = float(fin_sub_score)
    else:
        if roic is None or roic_bucket is None:
            return _factor_result(None, None, 0.2, 'f3_profitability', 'f3_weighted')
        r = float(roic)
        r_decimal = r / 100.0 if abs(r) > 1.0 else r
        spread = roic_minus_wacc
        spread_decimal = None if spread is None else float(spread) / 100.0 if abs(float(spread)) > 1.0 else float(spread)
        if roic_bucket == 'top_quartile':
            if spread_decimal is not None and spread_decimal > 0.05:
                raw = 10.0
            elif wacc is None:
                raw = 10.0
        elif roic_bucket == 'top_tercile':
            if spread_decimal is not None and spread_decimal > 0.0:
                raw = 8.0
            elif wacc is None:
                raw = 8.0
        elif roic_bucket == 'above_median' and spread_decimal is not None and spread_decimal < 0.0 and roic_trend == 'improving':
            raw = 5.0
        elif roic_bucket == 'below_median' and r_decimal < 0.10:
            raw = 1.0
        elif roic_bucket == 'below_median' and r_decimal > 0.0 and roic_trend in {'flat', 'stable'}:
            raw = 3.0
        elif roic_bucket == 'below_median' and r_decimal > 0.0:
            raw = 2.0
    if raw is None:
        return _factor_result(None, None, 0.2, 'f3_profitability', 'f3_weighted')
    modifiers = 0.0
    if roic_trend == 'improving':
        modifiers += 0.5
    elif roic_trend == 'deteriorating':
        modifiers -= 0.5
    if row.get('_persistent_fcf_ni_below_0_6') is True:
        modifiers -= 1.0
    adjusted = _clamp_score(raw + modifiers)
    if wacc is None and adjusted is not None:
        adjusted = min(adjusted, 8.0)
    return _factor_result(raw, adjusted, 0.2, 'f3_profitability', 'f3_weighted')

def score_factor_7(row: dict[str, Any]) -> dict[str, Any]:
    chs = row.get('chs_result')
    altman = row.get('altman_z_score')
    altman_variant = row.get('altman_model_variant')
    piotroski = row.get('piotroski_score')
    days_to_cover = row.get('days_to_cover')
    market_cap = row.get('market_cap')
    adr_flag = row.get('adr_flag')
    sector_class = row.get('financial_reit_utility_asset_light_flag')
    raw = None
    adjusted = None
    chs_only = sector_class in {'financial', 'reit', 'utility', 'asset_light'}
    chs_pass = None
    if chs is not None:
        norm = str(chs).strip().lower()
        if norm in CHS_PASS_VALUES:
            chs_pass = True
        elif norm in CHS_FAIL_VALUES:
            chs_pass = False
    piot_band = _piotroski_band_for_solvency(piotroski, market_cap)
    alt_band = _altman_band_for_solvency(altman, altman_variant)
    if chs_only:
        if chs_pass is not True or piotroski is None:
            return _factor_result(None, None, 0.05, 'f7_solvency', 'f7_weighted')
        p = float(piotroski)
        if p >= 7:
            raw = 8.0
        elif p in (5.0, 6.0):
            raw = 5.0
        elif p == 4.0:
            raw = 2.0
    else:
        if alt_band is None and piot_band is None:
            return _factor_result(None, None, 0.05, 'f7_solvency', 'f7_weighted')
        if alt_band is not None and piot_band is not None:
            raw = min(alt_band, piot_band)
        elif alt_band is not None:
            raw = alt_band
        else:
            raw = piot_band
        if raw == 10.0 and chs_pass is False:
            raw = None
    if raw is None:
        return _factor_result(None, None, 0.05, 'f7_solvency', 'f7_weighted')
    if adr_flag is True and raw > 8 and (piotroski is None or float(piotroski) < 8):
        raw = 8.0
    adjusted = raw
    if days_to_cover is not None and 7.0 <= float(days_to_cover) <= 15.0:
        if str(row.get('dtc_independence_check')).strip().lower() != 'waived':
            adjusted -= 1.0
    adjusted = _clamp_score(adjusted)
    return _factor_result(raw, adjusted, 0.05, 'f7_solvency', 'f7_weighted')

def main() -> None:
    args, output_dir, run_mode, selected_tickers, checkpoint = prepare_run(TICKER_LIST)
    if not selected_tickers:
        raise RuntimeError('No tickers configured. Populate TICKER_LIST or use --tickers.')
    remaining = get_remaining_tickers(selected_tickers, checkpoint)
    if remaining:
        new_rows = []
        for ticker in remaining:
            row = collect_one_ticker(ticker, 'manual', checkpoint['run_id'], run_mode)
            new_rows.append(row)
        all_rows = checkpoint_completed_rows_in_universe_order(checkpoint, selected_tickers) + new_rows
        scored_rows = score_and_finalize_rows(all_rows)
        checkpoint['rows_by_ticker'] = {}
        checkpoint['completed_tickers'] = []
        for row in scored_rows:
            t = _normalize_ticker(row['ticker'])
            checkpoint['rows_by_ticker'][t] = prepare_row_for_persist(row, checkpoint)
            checkpoint['completed_tickers'].append(t)
        checkpoint['completed_tickers'] = sorted(set(checkpoint['completed_tickers']))
        checkpoint['counts'] = _recompute_checkpoint_counts(checkpoint['rows_by_ticker'])
        checkpoint['last_updated_at_utc'] = _utc_now_iso()
        _atomic_write_json(_checkpoint_path(output_dir), checkpoint)
    manifest = finalize_run(output_dir, run_mode, selected_tickers, checkpoint)
    print(json.dumps({'done': True, 'run_mode': run_mode, 'checkpoint_summary': checkpoint_summary(checkpoint), 'manifest_path': _manifest_path(output_dir)}, indent=2))
SCRIPT_VERSION = 'v68_full_extractor_data_v18'
TOTAL_PORTFOLIO_CAPITAL = None
CURRENT_CONFIRMED_CATALYST_FLAG = None
CURRENT_MTUM_OUTPERFORMS_VTV_GT_5PP = None
CURRENT_SECTOR_THEME_PORTFOLIO_PCT = None
CURRENT_TRADE_TYPE = None
CURRENT_INSTITUTIONAL_CONVERGENCE_DAYS_SINCE_QE = None
CURRENT_CORRELATION_TO_TOP3_120D = None
CURRENT_CRASH_FLAG_ACTIVE = None
CURRENT_POSITION_COUNT = None
CURRENT_TIER_A_COUNT = None
CURRENT_SMALL_TIER_COUNT = None
CURRENT_MID_TIER_COUNT = None
CURRENT_SMALL_PLUS_MID_ALLOCATION_PCT = None
CURRENT_SWING_POSITION_COUNT = None
CURRENT_SWING_ALLOCATION_PCT = None
CURRENT_ADR_ALLOCATION_PCT = None
CURRENT_CHINESE_ADR_ALLOCATION_PCT = None
CURRENT_CASH_PCT = None
DOCUMENTED_PATH_TO_POSITIVE_FCF = None
MANUAL_GICS_4DIGIT_BY_TICKER = {}
MANUAL_DUAL_LISTING_FLAG_BY_TICKER = {}
MANUAL_CHINESE_ADR_FLAG_BY_TICKER = {}
MANUAL_ADR_FLAG_BY_TICKER = {}
MANUAL_SPAC_FLAG_BY_TICKER = {}
MANUAL_DOCUMENTED_PATH_TO_POSITIVE_FCF_BY_TICKER = {}
MANUAL_NIM_TREND_BY_TICKER = {}
MANUAL_FRE_GROWTH_PCT_BY_TICKER = {}
MANUAL_SI_PCT_AT_FILING_BY_TICKER = {}
MANUAL_INSTITUTIONAL_CONVERGENCE_FLAG_BY_TICKER = {}
MANUAL_OPTIONS_FLOW_FLAG_BY_TICKER = {}
MANUAL_SI_TRENDING_DOWN_FLAG_BY_TICKER = {}
MANUAL_TTM_INSIDER_BUY_VS_DILUTION_BY_TICKER = {}
MANUAL_NEXT_EARNINGS_DATE_BY_TICKER = {}
MANUAL_SPIN_OFF_FLAG_BY_TICKER = {}
MANUAL_TIKR_ROIC_BY_TICKER = {}
MANUAL_FINVIZ_PEG_RATIO_BY_TICKER = {}
MANUAL_FINVIZ_EPS_GROWTH_5Y_BY_TICKER = {}
MANUAL_TIKR_ECONOMIC_ROIC_BY_TICKER = {}
MANUAL_DTC_INDEPENDENCE_BY_TICKER = {}
MANUAL_REVISION_SOURCE_BY_TICKER = {}  # manual-only point-in-time revision source: {'TICKER': {'up_revisions': int, 'down_revisions': int, 'total_revisions': int}}
MANUAL_CHS_RESULT_BY_TICKER = {}  # manual-only CHS source for chs_only names; populate with values in CHS_PASS_VALUES / CHS_FAIL_VALUES
PEER_UNIVERSE_PATH = os.environ.get('PEER_UNIVERSE_PATH')
RUSSELL_2000_PROXY_TICKER = 'IWM'

def load_peer_universe_rows(path: Optional[str]=None) -> Optional[list[dict[str, Any]]]:
    src = path or PEER_UNIVERSE_PATH
    if not src:
        return None
    try:
        if src.lower().endswith('.json'):
            with open(src, 'r', encoding='utf-8') as f:
                payload = json.loads(f.read())
            if isinstance(payload, list):
                return payload
            return payload.get('rows') if isinstance(payload, dict) and isinstance(payload.get('rows'), list) else None
        rows = []
        with open(src, newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                rows.append(dict(row))
        return rows
    except Exception:
        return None

def _relative_return_bucket_to_quintile(relative_return_pct: float) -> Optional[int]:
    # ETF/Russell relative fallback is a scalar benchmark comparison, not a peer-distribution quintile.
    # Per Rule 54, do not invent a categorical proxy unless the system explicitly defines it.
    return None

CURRENT_PEER_UNIVERSE_ROWS = load_peer_universe_rows()
MIN_ADTV_BY_TIER = {'Large': 5000000.0, 'Mid': 1000000.0, 'Small': 300000.0}
MAX_BID_ASK_SPREAD_PCT_BY_TIER = {'Large': 0.2, 'Mid': 0.35, 'Small': 0.75}
TIER_B_SCORE_RANGE = 14.0
REGIME_SOFT_CRASH_DEDUCTION = -10.0

def _trading_dates_from_bars(bars: list[dict[str, Any]]) -> list[date]:
    out = []
    for b in bars or []:
        d = _extract_bar_date(b)
        if d is None:
            continue
        try:
            out.append(datetime.strptime(d, '%Y-%m-%d').date())
        except Exception:
            pass
    return sorted(set(out))

def _exact_trading_days_since_from_bars(bars: list[dict[str, Any]], start_date_str: Optional[str]) -> Optional[int]:
    if not bars or start_date_str is None:
        return None
    try:
        start = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    except Exception:
        return None
    return sum((1 for d in _trading_dates_from_bars(bars) if d > start))

def _exact_abs_trading_days_diff_from_bars(bars: list[dict[str, Any]], date_a_str: Optional[str], date_b_str: Optional[str]) -> Optional[int]:
    if not bars or date_a_str is None or date_b_str is None:
        return None
    try:
        a = datetime.strptime(date_a_str, '%Y-%m-%d').date()
        b = datetime.strptime(date_b_str, '%Y-%m-%d').date()
    except Exception:
        return None
    lo, hi = (a, b) if a <= b else (b, a)
    return sum((1 for d in _trading_dates_from_bars(bars) if lo < d <= hi))

def _compute_12_1_skip_month_exact(bars: list[dict[str, Any]]) -> Optional[float]:
    if not bars:
        return None
    month_to_last = {}
    for b in bars:
        d = _extract_bar_date(b)
        c = _extract_close_from_bar(b)
        if d is None or c is None:
            continue
        try:
            dt = datetime.strptime(d, '%Y-%m-%d').date()
        except Exception:
            continue
        key = (dt.year, dt.month)
        prev = month_to_last.get(key)
        if prev is None or dt > prev[0]:
            month_to_last[key] = (dt, c)
    if len(month_to_last) < 14:
        return None
    months = sorted(month_to_last.keys())
    start_close = month_to_last[months[-14]][1]
    end_close = month_to_last[months[-2]][1]
    return _pct_change(end_close, start_close)

def _valid_bar_count(bars: list[dict[str, Any]]) -> int:
    return sum(1 for b in bars if _extract_close_from_bar(b) is not None and _extract_bar_date(b) is not None)

def _select_preferred_bar_series(alpaca_payload: Any, massive_payload: Any, ticker: Optional[str]=None, min_required: int=273) -> list[dict[str, Any]]:
    alpaca_raw = _sort_bars_chronologically(_bars_from_alpaca(alpaca_payload or {}, ticker) if ticker else [])
    massive_raw = _sort_bars_chronologically(_bars_from_massive(massive_payload or {}))
    alpaca_count = _valid_bar_count(alpaca_raw)
    massive_count = _valid_bar_count(massive_raw)
    if alpaca_count < min_required and massive_count > alpaca_count:
        return massive_raw
    if alpaca_count == 0 and massive_count > 0:
        return massive_raw
    return alpaca_raw if alpaca_raw else massive_raw

def _factor5_director_fallback_threshold(market_cap_tier: Optional[str]) -> Optional[float]:
    if market_cap_tier == 'Large':
        return 500000.0
    if market_cap_tier == 'Mid':
        return 250000.0
    if market_cap_tier == 'Small':
        return 100000.0
    return None

def _max_position_pct_from_market_cap_tier(market_cap_tier: Optional[str]) -> Optional[float]:
    if market_cap_tier == 'Large':
        return 8.0
    if market_cap_tier == 'Mid':
        return 5.0
    if market_cap_tier == 'Small':
        return 1.0
    return None

def parse_price_and_trend_section(raw_alpaca_bars: Any, raw_alpaca_latest_bars: Any, raw_alpaca_latest_quotes: Any, raw_massive_bars: Any, raw_massive_snapshot: Any, raw_massive_last_trade: Any, raw_massive_last_quote: Any, ticker: str, sector_etf_ticker: Optional[str], raw_sector_alpaca_bars: Any, raw_sector_massive_bars: Any, sector_hv_median: Optional[float]=None) -> dict[str, Any]:
    out = _none_dict(['current_price', 'price_as_of', 'sma_50', 'sma_200', 'pct_vs_200sma', 'return_12_1_skip_month', 'hv30_annualized', 'adtv_20d_dollar', 'bid_ask_spread_pct', 'high_52w', 'high_52w_date', 'breakout_volume_ratio', 'up_day_volume_ratio_20d', 'golden_cross_flag', 'death_cross_flag', 'sector_etf_ticker', 'sector_etf_price', 'sector_etf_sma_200', 'sector_etf_above_200sma', 'sector_hv_median'])
    out['sector_etf_ticker'] = sector_etf_ticker
    out['sector_hv_median'] = sector_hv_median
    alpaca_bars = _sort_bars_chronologically(_bars_from_alpaca(raw_alpaca_bars, ticker))
    massive_bars = _sort_bars_chronologically(_bars_from_massive(raw_massive_bars))

    alpaca_count = _valid_bar_count(alpaca_bars)
    massive_count = _valid_bar_count(massive_bars)

    bars = alpaca_bars
    if alpaca_count < 273 and massive_count > alpaca_count:
        bars = massive_bars
    elif alpaca_count == 0 and massive_count > 0:
        bars = massive_bars
    closes = [_extract_close_from_bar(b) for b in bars]
    highs = [_extract_high_from_bar(b) for b in bars]
    vols = [_extract_volume_from_bar(b) for b in bars]
    dates = [_extract_bar_date(b) for b in bars]
    out['sma_50'] = _compute_sma(closes, 50)
    out['sma_200'] = _compute_sma(closes, 200)
    out['return_12_1_skip_month'] = _compute_12_1_skip_month_exact(bars)
    out['hv30_annualized'] = _compute_hv30_from_closes(closes)
    out['adtv_20d_dollar'] = _compute_adtv20(closes, vols)
    out['breakout_volume_ratio'] = _compute_breakout_volume_ratio(vols)
    out['up_day_volume_ratio_20d'] = _compute_up_down_volume_ratio_20d(closes, vols)
    if len(highs) >= 252 and len(dates) >= 252:
        highs_252 = highs[-252:]
        dates_252 = dates[-252:]
        valid_pairs = [(i, h) for i, h in enumerate(highs_252) if h is not None]
        if valid_pairs:
            max_idx = max(valid_pairs, key=lambda x: x[1])[0]
            out['high_52w'] = highs_252[max_idx]
            out['high_52w_date'] = dates_252[max_idx]
    latest_bar = _get_nested(raw_alpaca_latest_bars, 'bars', ticker)
    alpaca_current_price = None
    alpaca_price_as_of = None
    if isinstance(latest_bar, dict):
        alpaca_current_price = _to_float(_latest_non_null([latest_bar.get('c'), latest_bar.get('close')]))
        alpaca_price_as_of = _parse_iso_date(latest_bar.get('t')) or (dates[-1] if dates else None)
    massive_current_price = _latest_non_null([_to_float(_get_nested(raw_massive_snapshot, 'ticker', 'day', 'c')), _to_float(_get_nested(raw_massive_last_trade, 'results', 'p')), closes[-1] if closes else None])
    massive_price_as_of = _parse_iso_date(_get_nested(raw_massive_last_trade, 'results', 't')) or (dates[-1] if dates else None)
    out['current_price'] = alpaca_current_price if alpaca_current_price is not None else massive_current_price
    out['price_as_of'] = alpaca_price_as_of if alpaca_price_as_of is not None else massive_price_as_of
    quote = _get_nested(raw_alpaca_latest_quotes, 'quotes', ticker)
    alpaca_bid = None
    alpaca_ask = None
    if isinstance(quote, dict):
        alpaca_bid = _to_float(_latest_non_null([quote.get('bp'), quote.get('bid_price')]))
        alpaca_ask = _to_float(_latest_non_null([quote.get('ap'), quote.get('ask_price')]))
    massive_bid = _to_float(_get_nested(raw_massive_last_quote, 'results', 'P'))
    massive_ask = _to_float(_get_nested(raw_massive_last_quote, 'results', 'p'))
    bid = alpaca_bid if alpaca_bid is not None else massive_bid
    ask = alpaca_ask if alpaca_ask is not None else massive_ask
    if bid is not None and ask is not None and (ask + bid != 0):
        out['bid_ask_spread_pct'] = (ask - bid) / ((ask + bid) / 2.0) * 100.0
    if out['current_price'] is not None and out['sma_200'] not in (None, 0):
        out['pct_vs_200sma'] = (out['current_price'] / out['sma_200'] - 1.0) * 100.0
    out['golden_cross_flag'] = None if out['sma_50'] is None or out['sma_200'] is None else out['sma_50'] > out['sma_200']
    out['death_cross_flag'] = None if out['sma_50'] is None or out['sma_200'] is None else out['sma_50'] < out['sma_200']
    sector_bars = []
    if sector_etf_ticker:
        sector_alpaca_bars = _sort_bars_chronologically(_bars_from_alpaca(raw_sector_alpaca_bars, sector_etf_ticker))
        sector_massive_bars = _sort_bars_chronologically(_bars_from_massive(raw_sector_massive_bars))
        sector_alpaca_count = _valid_bar_count(sector_alpaca_bars)
        sector_massive_count = _valid_bar_count(sector_massive_bars)
        sector_bars = sector_alpaca_bars
        if sector_alpaca_count < 200 and sector_massive_count > sector_alpaca_count:
            sector_bars = sector_massive_bars
        elif sector_alpaca_count == 0 and sector_massive_count > 0:
            sector_bars = sector_massive_bars
    sector_closes = [_extract_close_from_bar(b) for b in sector_bars]
    out['sector_etf_price'] = sector_closes[-1] if sector_closes else None
    out['sector_etf_sma_200'] = _compute_sma(sector_closes, 200)
    out['sector_etf_above_200sma'] = None if out['sector_etf_price'] is None or out['sector_etf_sma_200'] is None else out['sector_etf_price'] > out['sector_etf_sma_200']
    out['_sector_etf_return_12_1_skip_month'] = _compute_12_1_skip_month_exact(sector_bars)
    return out

def parse_earnings_quality_section(raw_fmp_income_ttm: Any, raw_fmp_cash_flow_ttm_like: Any, raw_fmp_balance_annual: Any, raw_fmp_balance_quarter: Any, raw_fmp_income_quarter: Any) -> dict[str, Any]:
    out = _none_dict(['net_income_ttm', 'ocf_ttm', 'revenue_ttm', 'total_assets_cur', 'total_assets_prior', 'sloan_ratio', 'ocf_ni_ratio', 'ocf_revenue_ratio', 'sector_sloan_threshold', 'sector_ocf_revenue_median', 'etr_change_qoq', 'ar_growth_vs_revenue', 'inventory_growth_vs_revenue', 'asset_growth_yoy', 'net_dilution_pct', 'persistent_ocf_ni_divergence_flag', 'revenue_vs_earnings_divergence_flag'])
    income_rows = raw_fmp_income_ttm if isinstance(raw_fmp_income_ttm, list) else []
    cf_rows = raw_fmp_cash_flow_ttm_like if isinstance(raw_fmp_cash_flow_ttm_like, list) else []
    bal_a = raw_fmp_balance_annual if isinstance(raw_fmp_balance_annual, list) else []
    bal_q = raw_fmp_balance_quarter if isinstance(raw_fmp_balance_quarter, list) else []
    inc_q = raw_fmp_income_quarter if isinstance(raw_fmp_income_quarter, list) else []

    def _stmt_row_sort_key(r: dict) -> str:
        return str(_latest_non_null([
            r.get('date'),
            r.get('fillingDate'),
            r.get('acceptedDate'),
            r.get('calendarYear'),
            r.get('period'),
        ]) or '0000-00-00')

    income_rows = sorted(income_rows, key=_stmt_row_sort_key, reverse=True)
    cf_rows = sorted(cf_rows, key=_stmt_row_sort_key, reverse=True)
    bal_a = sorted(bal_a, key=_stmt_row_sort_key, reverse=True)
    bal_q = sorted(bal_q, key=_stmt_row_sort_key, reverse=True)
    inc_q = sorted(inc_q, key=_stmt_row_sort_key, reverse=True)
    out['net_income_ttm'] = _sum_latest_quarters(income_rows, 'netIncome', 4)
    _ocf_primary = _sum_latest_quarters(cf_rows, 'operatingCashFlow', 4)
    _ocf_fallback = _sum_latest_quarters(cf_rows, 'netCashProvidedByOperatingActivities', 4)
    out['ocf_ttm'] = _ocf_primary if _ocf_primary is not None else _ocf_fallback
    out['revenue_ttm'] = _sum_latest_quarters(income_rows, 'revenue', 4)
    if bal_a:
        out['total_assets_cur'] = _to_float(bal_a[0].get('totalAssets'))
        if len(bal_a) > 1:
            out['total_assets_prior'] = _to_float(bal_a[1].get('totalAssets'))
    avg_assets = (out['total_assets_cur'] + out['total_assets_prior']) / 2.0 if out['total_assets_cur'] is not None and out['total_assets_prior'] is not None else None
    if out['net_income_ttm'] is not None and out['ocf_ttm'] is not None and (avg_assets not in (None, 0)):
        out['sloan_ratio'] = (out['net_income_ttm'] - out['ocf_ttm']) / avg_assets
    out['ocf_ni_ratio'] = _safe_div(out['ocf_ttm'], out['net_income_ttm'])
    out['ocf_revenue_ratio'] = _safe_div(out['ocf_ttm'], out['revenue_ttm'])
    out['sector_sloan_threshold'] = None
    out['sector_ocf_revenue_median'] = None
    if len(inc_q) >= 2:
        latest_q = inc_q[0]
        prior_q = inc_q[1]
        pretax_latest = _to_float(latest_q.get('incomeBeforeTax'))
        tax_latest = _to_float(latest_q.get('incomeTaxExpense'))
        pretax_prior = _to_float(prior_q.get('incomeBeforeTax'))
        tax_prior = _to_float(prior_q.get('incomeTaxExpense'))
        etr_latest = _safe_div(tax_latest, pretax_latest)
        etr_prior = _safe_div(tax_prior, pretax_prior)
        if etr_latest is not None and etr_prior is not None:
            out['etr_change_qoq'] = (etr_latest - etr_prior) * 100.0
    if len(bal_q) >= 2 and len(inc_q) >= 2:
        latest_bq = bal_q[0]
        prior_bq = bal_q[1]
        latest_iq = inc_q[0]
        prior_iq = inc_q[1]
        rev_growth = _pct_change(_to_float(latest_iq.get('revenue')), _to_float(prior_iq.get('revenue')))
        ar_growth = _pct_change(_to_float(latest_bq.get('netReceivables')), _to_float(prior_bq.get('netReceivables')))
        inv_growth = _pct_change(_to_float(latest_bq.get('inventory')), _to_float(prior_bq.get('inventory')))
        if ar_growth is not None and rev_growth is not None:
            out['ar_growth_vs_revenue'] = ar_growth - rev_growth
        if inv_growth is not None and rev_growth is not None:
            out['inventory_growth_vs_revenue'] = inv_growth - rev_growth
    if len(bal_a) >= 2:
        out['asset_growth_yoy'] = _pct_change(_to_float(bal_a[0].get('totalAssets')), _to_float(bal_a[1].get('totalAssets')))
    first_income = income_rows[0] if income_rows else {}
    shares_cur = _to_float(first_income.get('weightedAverageShsOut')) if isinstance(first_income, dict) else None
    shares_prior = _to_float(inc_q[4].get('weightedAverageShsOut')) if len(inc_q) >= 5 else None
    out['net_dilution_pct'] = _pct_change(shares_cur, shares_prior)
    quarterly_ratios = []
    for i in range(min(len(cf_rows), len(inc_q), 4)):
        ocf = _latest_non_null([
            _to_float(cf_rows[i].get('operatingCashFlow')),
            _to_float(cf_rows[i].get('netCashProvidedByOperatingActivities')),
        ])
        ni = _to_float(inc_q[i].get('netIncome'))
        if ni is not None and ni < 0:
            quarterly_ratios.append(None)
            continue
        ratio = _safe_div(ocf, ni)
        quarterly_ratios.append(ratio)

    valid_flags = [r is not None and r < 0.6 for r in quarterly_ratios if r is not None]

    if len(valid_flags) >= 2:
        out['persistent_ocf_ni_divergence_flag'] = sum(valid_flags) >= 2
    else:
        out['persistent_ocf_ni_divergence_flag'] = None
    if len(inc_q) >= 3:
        q1 = inc_q[0]
        q2 = inc_q[1]
        q3 = inc_q[2]
        rev_growth_1 = _pct_change(_to_float(q1.get('revenue')), _to_float(q2.get('revenue')))
        ni_growth_1 = _pct_change(_to_float(q1.get('netIncome')), _to_float(q2.get('netIncome')))
        rev_growth_2 = _pct_change(_to_float(q2.get('revenue')), _to_float(q3.get('revenue')))
        ni_growth_2 = _pct_change(_to_float(q2.get('netIncome')), _to_float(q3.get('netIncome')))
        if None not in (rev_growth_1, ni_growth_1, rev_growth_2, ni_growth_2):
            earnings_outpacing = ni_growth_1 > rev_growth_1 and ni_growth_2 > rev_growth_2
            if earnings_outpacing:
                gm_q1 = _safe_div(_to_float(q1.get('grossProfit')), _to_float(q1.get('revenue')))
                gm_q2 = _safe_div(_to_float(q2.get('grossProfit')), _to_float(q2.get('revenue')))
                gross_margin_improving = gm_q1 is not None and gm_q2 is not None and gm_q1 > gm_q2
                out['revenue_vs_earnings_divergence_flag'] = not gross_margin_improving
            else:
                out['revenue_vs_earnings_divergence_flag'] = False
    return out

def parse_insider_section(raw_finnhub_insider_transactions: Any, raw_sec_filing_text: Optional[str], current_price: Optional[float], high_52w: Optional[float]) -> dict[str, Any]:
    fields = ['latest_insider_filing_date', 'latest_insider_name', 'latest_insider_title', 'latest_insider_transaction_code', 'latest_insider_shares', 'latest_insider_value', 'insider_holdings_pct', 'insider_holdings_verified', 'insider_fallback_threshold_used', 'sec_open_market_confirmed', 'sec_10b5_1_mentioned', 'sec_10b5_1_regime', 'sec_10b5_1_adoption_date', 'cluster_buy_flag', 'cluster_buy_count', 'cluster_buy_members', 'signal_age_days', 'si_pct_at_filing', 'buy_within_10d_earnings_flag', 'institutional_convergence_flag', 'options_flow_flag', 'si_trending_down_flag', 'ttm_insider_buy_vs_dilution', 'sector_underperformance_24m', 'ceo_cfo_drawdown_buy_flag', '_cluster_open_market_best_available']
    out = _none_dict(fields)
    data = _get_nested(raw_finnhub_insider_transactions, 'data')
    rows = data if isinstance(data, list) else []
    cluster_titles_all = {}
    cluster_member_titles = {}
    latest_buy_person = None
    latest_buy_date = None
    latest_buy_row = None
    ceo_cfo_self_sale_within_90d = False
    def _row_sort_key(r: dict) -> str:
        d = _parse_iso_date(_latest_non_null([r.get('transactionDate'), r.get('filingDate')]))
        return d or '0000-00-00'
    rows = sorted(rows, key=_row_sort_key, reverse=True)
    names_60: list[str] = []
    names_30: list[str] = []
    if rows:
        latest_overall_row = rows[0]
        for _r in rows:
            _code = str(_latest_non_null([_r.get('transactionCode'), _r.get('transactionType')]) or '').upper()
            if _code in {'P', 'BUY'}:
                latest_buy_row = _r
                latest_buy_person = str(_r.get('name') or _r.get('personName') or '').strip() or None
                latest_buy_date = _parse_iso_date(_latest_non_null([_r.get('transactionDate'), _r.get('filingDate')]))
                break
        tx = latest_buy_row or latest_overall_row
        out['_latest_buy_date'] = latest_buy_date
        out['latest_insider_filing_date'] = _parse_iso_date(_latest_non_null([tx.get('filingDate'), tx.get('transactionDate')]))
        out['latest_insider_name'] = _latest_non_null([tx.get('name'), tx.get('personName')])
        out['latest_insider_title'] = _latest_non_null([tx.get('title'), tx.get('position')])
        out['latest_insider_transaction_code'] = _latest_non_null([tx.get('transactionCode'), tx.get('transactionType')])
        out['latest_insider_shares'] = _latest_non_null([_to_float(tx.get('share')), _to_float(tx.get('shares')), _to_float(tx.get('change'))])
        out['latest_insider_value'] = _latest_non_null([_to_float(tx.get('value')), _to_float(tx.get('transactionValue'))])
        today = date.today()
        open_buy_rows_60 = []
        open_buy_rows_30 = []
        for r in rows:
            code = str(_latest_non_null([r.get('transactionCode'), r.get('transactionType')]) or '').upper()
            filing_date = _parse_iso_date(_latest_non_null([r.get('filingDate'), r.get('transactionDate')]))
            name = str(r.get('name') or r.get('personName') or '').strip()
            title = str(r.get('title') or r.get('position') or '').strip()
            if name and title:
                cluster_titles_all[name] = title
            try:
                fd = datetime.strptime(filing_date, '%Y-%m-%d').date() if filing_date is not None else None
            except Exception:
                fd = None
            if code in {'P', 'BUY'} and fd is not None:
                age = (today - fd).days
                if age <= 60:
                    open_buy_rows_60.append(r)
                    if name and title:
                        cluster_member_titles[name] = title
                if age <= 30:
                    open_buy_rows_30.append(r)
                    if name and title:
                        cluster_member_titles[name] = title
            if latest_buy_person and latest_buy_date and fd is not None and name == latest_buy_person and code in {'S', 'SALE'}:
                try:
                    lbd = datetime.strptime(latest_buy_date, '%Y-%m-%d').date()
                except Exception:
                    lbd = None
                if lbd is not None and 0 < (fd - lbd).days <= 90 and any((x in title.lower() for x in ['chief executive', 'ceo', 'chief financial', 'cfo'])):
                    ceo_cfo_self_sale_within_90d = True
        names_60 = sorted(set((str(r.get('name') or r.get('personName') or '').strip() for r in open_buy_rows_60 if r.get('name') or r.get('personName'))))
        names_30 = sorted(set((str(r.get('name') or r.get('personName') or '').strip() for r in open_buy_rows_30 if r.get('name') or r.get('personName'))))
        if len(names_30) >= 3:
            out['cluster_buy_flag'] = True
            out['cluster_buy_count'] = len(names_30)
            out['cluster_buy_members'] = '|'.join(names_30)
        elif len(names_60) >= 2:
            out['cluster_buy_flag'] = True
            out['cluster_buy_count'] = len(names_60)
            out['cluster_buy_members'] = '|'.join(names_60)
        else:
            out['cluster_buy_flag'] = False
            out['cluster_buy_count'] = max(len(names_30), len(names_60))
            out['cluster_buy_members'] = '|'.join(names_60) if names_60 else None
    selected_names = names_30 if len(names_30) >= 3 else names_60
    selected_rows = open_buy_rows_30 if len(names_30) >= 3 else open_buy_rows_60
    out['_cluster_member_titles'] = {name: cluster_member_titles.get(name) for name in selected_names if name in cluster_member_titles}
    out['_cluster_open_market_best_available'] = False
    out['_ceo_cfo_self_sale_within_90d'] = ceo_cfo_self_sale_within_90d
    if latest_buy_date is not None:
        try:
            d = datetime.strptime(latest_buy_date, '%Y-%m-%d').date()
            out['signal_age_days'] = max((date.today() - d).days, 0)
        except Exception:
            pass
    if raw_sec_filing_text:
        txt = raw_sec_filing_text
        txt_up = txt.upper()
        out['sec_10b5_1_mentioned'] = '10B5-1' in txt_up or '10B5 1' in txt_up
        out['sec_10b5_1_adoption_date'] = _extract_10b5_1_adoption_date(txt)
        if out['sec_10b5_1_mentioned']:
            out['sec_10b5_1_regime'] = 'post_reform_confirmed' if out['sec_10b5_1_adoption_date'] and out['sec_10b5_1_adoption_date'] >= '2023-02-27' else 'pre_reform_or_unverifiable'
        # sec_open_market_confirmed reflects EDGAR confirmation of the selected filing only.
        # cluster_buy_flag is established independently from insider transaction data.
        out['sec_open_market_confirmed'] = 'OPEN MARKET' in txt_up
        shares_owned_after = _extract_text_number(txt, r'shares\s+owned\s+following\s+reported\s+transaction\(s\)\s*[:\-]?\s*([0-9,\.]+)')
        if shares_owned_after is not None and out['latest_insider_shares'] is not None:
            owned = _to_float(shares_owned_after.replace(',', ''))
            if owned not in (None, 0):
                out['insider_holdings_pct'] = out['latest_insider_shares'] / owned * 100.0
                out['insider_holdings_verified'] = True
                out['insider_fallback_threshold_used'] = False
    if out['insider_holdings_verified'] is None:
        out['insider_holdings_verified'] = False
        out['insider_fallback_threshold_used'] = True
    ticker = None
    if isinstance(raw_finnhub_insider_transactions, dict):
        ticker = str(raw_finnhub_insider_transactions.get('symbol') or raw_finnhub_insider_transactions.get('ticker') or '').strip().upper() or None
    out['si_pct_at_filing'] = MANUAL_SI_PCT_AT_FILING_BY_TICKER.get(ticker or '', None)
    out['institutional_convergence_flag'] = MANUAL_INSTITUTIONAL_CONVERGENCE_FLAG_BY_TICKER.get(ticker or '', None)
    out['options_flow_flag'] = MANUAL_OPTIONS_FLOW_FLAG_BY_TICKER.get(ticker or '', None)
    out['si_trending_down_flag'] = MANUAL_SI_TRENDING_DOWN_FLAG_BY_TICKER.get(ticker or '', None)
    out['ttm_insider_buy_vs_dilution'] = MANUAL_TTM_INSIDER_BUY_VS_DILUTION_BY_TICKER.get(ticker or '', None)
    title = (out['latest_insider_title'] or '').lower()
    tx_code = str(out.get('latest_insider_transaction_code') or '').upper()
    if any((x in title for x in ['chief executive', 'ceo', 'chief financial', 'cfo'])) and tx_code in {'P', 'BUY'}:
        if out['sec_open_market_confirmed'] is True and current_price is not None and high_52w not in (None, 0):
            drawdown = (current_price / high_52w - 1.0) * 100.0
            out['ceo_cfo_drawdown_buy_flag'] = drawdown <= -20.0
        elif out['sec_open_market_confirmed'] is None:
            out['ceo_cfo_drawdown_buy_flag'] = None
    return out

def enrich_cross_section(rows: list[dict[str, Any]], peer_rows: Optional[list[dict[str, Any]]]=None) -> None:
    treasury_change = _fred_10y_30d_change_bps()
    peer_source = peer_rows if isinstance(peer_rows, list) and peer_rows else []
    gics_groups_return: dict[str, list[float]] = {}
    sector_groups_return: dict[str, list[float]] = {}
    sector_groups_roic: dict[str, list[float]] = {}
    sector_groups_fcf_yield: dict[str, list[float]] = {}
    gics_groups_24m: dict[str, list[float]] = {}
    sector_groups_hv: dict[str, list[float]] = {}
    sector_groups_ev: dict[str, list[float]] = {}
    sector_groups_gpa: dict[str, list[float]] = {}
    gics_groups_gpa: dict[str, list[float]] = {}
    sector_groups_ps: dict[str, list[float]] = {}
    sector_groups_ocf_rev: dict[str, list[float]] = {}
    for row in peer_source:
        sector = row.get('sector')
        gics = row.get('gics_4digit') or MANUAL_GICS_4DIGIT_BY_TICKER.get(str(row.get('ticker') or '').strip().upper())
        ret = row.get('return_12_1_skip_month')
        if gics and ret is not None:
            gics_groups_return.setdefault(str(gics), []).append(float(ret))
        if sector and ret is not None:
            sector_groups_return.setdefault(str(sector), []).append(float(ret))
        if sector and row.get('roic') is not None:
            try:
                sector_groups_roic.setdefault(str(sector), []).append(float(row['roic']))
            except Exception:
                pass
        if sector and row.get('_fcf_yield') is not None:
            try:
                sector_groups_fcf_yield.setdefault(str(sector), []).append(float(row['_fcf_yield']))
            except Exception:
                pass
        if gics and row.get('_stock_ret_24m_anchor') is not None:
            try:
                gics_groups_24m.setdefault(str(gics), []).append(float(row['_stock_ret_24m_anchor']))
            except Exception:
                pass
        if sector and row.get('hv30_annualized') is not None:
            try:
                sector_groups_hv.setdefault(str(sector), []).append(float(row['hv30_annualized']))
            except Exception:
                pass
        if sector and row.get('ev_ebitda') is not None:
            try:
                sector_groups_ev.setdefault(str(sector), []).append(float(row['ev_ebitda']))
            except Exception:
                pass
        if sector and row.get('gpa_ratio') is not None:
            try:
                sector_groups_gpa.setdefault(str(sector), []).append(float(row['gpa_ratio']))
            except Exception:
                pass
        if gics and row.get('gpa_ratio') is not None:
            try:
                gics_groups_gpa.setdefault(str(gics), []).append(float(row['gpa_ratio']))
            except Exception:
                pass
        if sector and row.get('price_to_sales') is not None:
            try:
                sector_groups_ps.setdefault(str(sector), []).append(float(row['price_to_sales']))
            except Exception:
                pass
        if sector and row.get('ocf_revenue_ratio') is not None:
            try:
                sector_groups_ocf_rev.setdefault(str(sector), []).append(float(row['ocf_revenue_ratio']))
            except Exception:
                pass
    for row in rows:
        t = str(row.get('ticker') or '').strip().upper()
        sector = row.get('sector')
        gics = row.get('gics_4digit') or MANUAL_GICS_4DIGIT_BY_TICKER.get(t)
        row['gics_4digit'] = gics
        row['sector_hv_median'] = statistics.median(sector_groups_hv[str(sector)]) if sector in sector_groups_hv and sector_groups_hv[str(sector)] else None
        row['ev_ebitda_sector_median'] = statistics.median(sector_groups_ev[str(sector)]) if sector in sector_groups_ev and sector_groups_ev[str(sector)] else None
        if row.get('_requires_gpa_peer_benchmark') is True and gics in gics_groups_gpa and gics_groups_gpa[str(gics)]:
            row['gpa_sector_median'] = statistics.median(gics_groups_gpa[str(gics)])
        else:
            row['gpa_sector_median'] = statistics.median(sector_groups_gpa[str(sector)]) if sector in sector_groups_gpa and sector_groups_gpa[str(sector)] else None
        row['ps_sector_median'] = statistics.median(sector_groups_ps[str(sector)]) if sector in sector_groups_ps and sector_groups_ps[str(sector)] else None
        row['sector_ocf_revenue_median'] = statistics.median(sector_groups_ocf_rev[str(sector)]) if sector in sector_groups_ocf_rev and sector_groups_ocf_rev[str(sector)] else None
        group = None
        row['_momentum_peer_method'] = None
        if row.get('return_12_1_skip_month') is not None:
            tier = row.get('market_cap_tier')
            gics_group = sorted(gics_groups_return.get(str(gics), [])) if gics else []
            min_peers = 3 if tier == 'Mid' else 2 if tier == 'Small' else 1
            if gics and len(gics_group) >= min_peers:
                group = gics_group
                row['_momentum_peer_method'] = 'gics_4digit'
            if group:
                val = float(row['return_12_1_skip_month'])
                row['_momentum_quintile'] = _quintile_rank(group, val)
            else:
                val = float(row['return_12_1_skip_month'])
                sector_etf_ret = row.get('_sector_etf_return_12_1_skip_month')
                russell_ret = row.get('_russell2000_return_12_1_skip_month')
                if tier in {'Mid', 'Small'} and sector_etf_ret is not None:
                    row['_momentum_peer_method'] = 'sector_etf_relative'
                    row['_momentum_quintile'] = _relative_return_bucket_to_quintile(val - float(sector_etf_ret))
                elif tier in {'Mid', 'Small'} and russell_ret is not None:
                    row['_momentum_peer_method'] = 'russell_2000_relative'
                    row['_momentum_quintile'] = _relative_return_bucket_to_quintile(val - float(russell_ret))
                else:
                    row['_momentum_peer_method'] = None
                    row['_momentum_quintile'] = None
            sector_group = sorted(sector_groups_return.get(str(sector), [])) if sector else []
            if sector_group:
                val = float(row['return_12_1_skip_month'])
                pct = sum((1 for x in sector_group if x <= val)) / len(sector_group)
                row['_top_10pct_sector_momentum'] = pct >= 0.9
                row['_top_20pct_sector_momentum'] = pct >= 0.8
            else:
                row['_top_10pct_sector_momentum'] = None
                row['_top_20pct_sector_momentum'] = None
        if sector and row.get('roic') is not None:
            vals = sorted(sector_groups_roic.get(str(sector), []))
            if vals:
                r = float(row['roic'])
                q75 = vals[max(0, int(len(vals) * 0.75) - 1)]
                q66 = vals[max(0, int(len(vals) * 0.66) - 1)]
                med = vals[max(0, int(len(vals) * 0.5) - 1)]
                if r >= q75:
                    row['_roic_vs_sector_bucket'] = 'top_quartile'
                elif r >= q66:
                    row['_roic_vs_sector_bucket'] = 'top_tercile'
                elif r >= med:
                    row['_roic_vs_sector_bucket'] = 'above_median'
                else:
                    row['_roic_vs_sector_bucket'] = 'below_median'
                row['_roic_top_quartile'] = row['_roic_vs_sector_bucket'] == 'top_quartile'
            else:
                row['_roic_vs_sector_bucket'] = None
                row['_roic_top_quartile'] = None
        row['_persistent_fcf_ni_below_0_6'] = None if row.get('fcf_ni_ratio') is None else float(row['fcf_ni_ratio']) < 0.6
        fin_sub = None
        industry_l = str(row.get('industry') or '').lower()
        roe = row.get('roe')
        nim_trend = row.get('nim_trend')
        fre_growth_pct = row.get('fre_growth_pct')
        roe_decimal = None if roe is None else float(roe) / 100.0 if abs(float(roe)) > 1.0 else float(roe)
        if 'bank' in industry_l:
            if roe_decimal is not None and roe_decimal > 0.12 and nim_trend in {'stable', 'improving'}:
                fin_sub = 5.0
        elif any((x in industry_l for x in ['asset manager', 'asset management', 'bdc', 'capital markets'])):
            if fre_growth_pct is not None and float(fre_growth_pct) > 10.0:
                fin_sub = 5.0
        row['_financial_substitution_score'] = fin_sub
        fcf_ratio = row.get('fcf_ni_ratio')
        net_income_ttm = row.get('net_income_ttm')
        market_cap_val = row.get('market_cap')
        if fcf_ratio is not None and net_income_ttm is not None and market_cap_val not in (None, 0):
            try:
                row['_fcf_yield'] = float(fcf_ratio) * float(net_income_ttm) / float(market_cap_val)
            except Exception:
                row['_fcf_yield'] = None
        else:
            row['_fcf_yield'] = None
        if sector and row.get('_fcf_yield') is not None:
            fcf_vals = sorted(sector_groups_fcf_yield.get(str(sector), []))
            if fcf_vals:
                fcf_pct = sum((1 for x in fcf_vals if x <= float(row['_fcf_yield']))) / len(fcf_vals)
                row['_fcf_yield_top_10_pct_sector'] = fcf_pct >= 0.9
            else:
                row['_fcf_yield_top_10_pct_sector'] = None
        else:
            row['_fcf_yield_top_10_pct_sector'] = None
        titles = row.get('_cluster_member_titles') or {}
        if isinstance(titles, dict) and row.get('cluster_buy_flag') is True:
            vals = [str(v or '').lower() for v in titles.values()]
            row['_cluster_includes_ceo_cfo'] = any((any((k in tt for k in ['chief executive', 'ceo', 'chief financial', 'cfo'])) for tt in vals))
            row['_cluster_director_only'] = len(vals) > 0 and (not row['_cluster_includes_ceo_cfo']) and all(('director' in tt for tt in vals))
        else:
            row['_cluster_includes_ceo_cfo'] = None
            row['_cluster_director_only'] = None
        days_since_qe = CURRENT_INSTITUTIONAL_CONVERGENCE_DAYS_SINCE_QE
        row['_institutional_convergence_days_since_qe'] = days_since_qe
        if row.get('institutional_convergence_flag') is True:
            if days_since_qe is not None:
                d = float(days_since_qe)
                row['_institutional_convergence_adjustment'] = 1.0 if d <= 90 else 0.5 if d <= 180 else 0.0
            else:
                row['_institutional_convergence_adjustment'] = None
        else:
            row['_institutional_convergence_adjustment'] = 0.0 if row.get('institutional_convergence_flag') is False else None
        row['_confirmed_catalyst_flag'] = CURRENT_CONFIRMED_CATALYST_FLAG
        row['_trade_type'] = CURRENT_TRADE_TYPE
        row['_is_swing_trade'] = str(CURRENT_TRADE_TYPE).strip().lower() == 'swing' if CURRENT_TRADE_TYPE is not None else None
        row['_mtum_outperforms_vtv_gt_5pp'] = CURRENT_MTUM_OUTPERFORMS_VTV_GT_5PP
        row['_sector_theme_portfolio_pct'] = CURRENT_SECTOR_THEME_PORTFOLIO_PCT
        row['_correlation_to_top3_120d'] = CURRENT_CORRELATION_TO_TOP3_120D
        row['treasury_10y_30d_change_bps'] = treasury_change
        row['soft_crash_flag_active'] = CURRENT_SOFT_CRASH_FLAG_ACTIVE
        row['full_crash_flag_active'] = CURRENT_FULL_CRASH_FLAG_ACTIVE
        row['mtum_vtv_cap_active'] = CURRENT_MTUM_VTV_CAP_ACTIVE
        row['hy_oas_bear_rally_active'] = CURRENT_HY_OAS_BEAR_RALLY_ACTIVE
        row['bear_rally_constraint_active'] = row.get('hy_oas_bear_rally_active')
        peg = row.get('peg_ratio')
        ev = row.get('ev_ebitda')
        ev_med = row.get('ev_ebitda_sector_median')
        ps = row.get('price_to_sales')
        ps_med = row.get('ps_sector_median')
        triggers = 0
        if peg is not None and 1.5 <= float(peg) <= 2.5:
            triggers += 1
        if ev is not None and ev_med not in (None, 0) and 1.5 * float(ev_med) <= float(ev) <= 2.0 * float(ev_med):
            triggers += 1
        non_saas = not bool(row.get('saas_high_growth_flag'))
        if ps is not None and ps_med not in (None, 0) and float(ps) < 30.0:
            if (non_saas and float(ps) > 3.0 * float(ps_med)) or ((not non_saas) and float(ps) > float(ps_med)):
                triggers += 1
        row['valuation_soft_flag'] = triggers >= 2
        gics_24m_vals = list(gics_groups_24m.get(str(gics), [])) if gics and gics_groups_24m.get(str(gics)) else []
        stock_ret_24m_anchor = row.get('_stock_ret_24m_anchor')
        sector_ret_24m_anchor = row.get('_sector_etf_ret_24m_anchor')
        benchmark_ret_24m = None
        if stock_ret_24m_anchor is not None:
            if len(gics_24m_vals) >= 5:
                benchmark_ret_24m = statistics.median(gics_24m_vals)
            elif sector_ret_24m_anchor is not None:
                benchmark_ret_24m = float(sector_ret_24m_anchor)
        if stock_ret_24m_anchor is None or benchmark_ret_24m is None or 1 + float(stock_ret_24m_anchor) <= 0 or 1 + float(benchmark_ret_24m) <= 0:
            row['sector_underperformance_24m'] = None
        else:
            row['sector_underperformance_24m'] = (((1 + float(stock_ret_24m_anchor)) / (1 + float(benchmark_ret_24m))) ** 0.5 - 1.0) * 100.0

def apply_hard_floors(row: dict[str, Any]) -> tuple[str, list[str]]:
    fail_reasons = []
    incomplete_reasons = []
    market_cap = row.get('market_cap')
    if market_cap is None:
        incomplete_reasons.append('missing_market_cap')
    elif float(market_cap) < MIN_MARKET_CAP:
        fail_reasons.append('market_cap_below_50m')
    pct_vs_200sma = row.get('pct_vs_200sma')
    if pct_vs_200sma is None:
        current_price = row.get('current_price')
        sma_200 = row.get('sma_200')
        if current_price is None or sma_200 in (None, 0):
            incomplete_reasons.append('missing_pct_vs_200sma')
        else:
            pct_vs_200sma = (float(current_price) / float(sma_200) - 1.0) * 100.0
    if pct_vs_200sma is not None and float(pct_vs_200sma) < MAX_PCT_BELOW_200SMA:
        fail_reasons.append('price_more_than_30pct_below_200sma')
    saas_flag = row.get('saas_high_growth_flag')
    price_to_sales = row.get('price_to_sales')
    if saas_flag is True:
        if price_to_sales is None:
            incomplete_reasons.append('missing_price_to_sales_for_saas_high_growth')
        elif float(price_to_sales) > PS_HARD_FLOOR_CEILING:
            fail_reasons.append('price_to_sales_above_30x_saas_high_growth')
    elif saas_flag is None and price_to_sales is not None and float(price_to_sales) > PS_HARD_FLOOR_CEILING:
        incomplete_reasons.append('unknown_saas_high_growth_flag_for_ps_hard_floor')
    fcf_streak = row.get('free_cash_flow_negative_3y_streak')
    t = str(row.get('ticker') or '').strip().upper()
    documented_path = MANUAL_DOCUMENTED_PATH_TO_POSITIVE_FCF_BY_TICKER.get(t, DOCUMENTED_PATH_TO_POSITIVE_FCF)
    if fcf_streak is None:
        incomplete_reasons.append('missing_free_cash_flow_negative_3y_streak')
    elif fcf_streak is True:
        if documented_path is False:
            fail_reasons.append('negative_fcf_3y_no_documented_path')
        elif documented_path is None:
            incomplete_reasons.append('missing_documented_path_to_positive_fcf')
    altman_variant = row.get('altman_model_variant')
    piotroski_score = row.get('piotroski_score')
    if altman_variant == '1968_original':
        altman_z = row.get('altman_z_score')
        if altman_z is None:
            incomplete_reasons.append('missing_altman_z_score_1968_original')
        elif float(altman_z) < ALTMAN_1968_MIN_PASS:
            fail_reasons.append('altman_z_below_1_81')
        if piotroski_score is None:
            incomplete_reasons.append('missing_piotroski_score')
        elif float(piotroski_score) < PIOTROSKI_MIN_PASS:
            fail_reasons.append('piotroski_score_3_or_below')
    elif altman_variant == '1995_z_double_prime':
        altman_z = row.get('altman_z_score')
        if altman_z is None:
            incomplete_reasons.append('missing_altman_z_score_z_double_prime')
        elif float(altman_z) < ALTMAN_ZPP_MIN_PASS:
            fail_reasons.append('altman_z_double_prime_below_1_10')
        if piotroski_score is None:
            incomplete_reasons.append('missing_piotroski_score')
        elif float(piotroski_score) < PIOTROSKI_MIN_PASS:
            fail_reasons.append('piotroski_score_3_or_below')
    elif altman_variant == 'chs_only':
        chs_class = _classify_chs_result(row.get('chs_result'))
        if chs_class is False:
            fail_reasons.append('chs_fail')
        elif chs_class is None:
            incomplete_reasons.append('missing_or_ambiguous_chs_result')
            if piotroski_score is None:
                incomplete_reasons.append('missing_piotroski_score_chs_fallback')
            elif float(piotroski_score) < PIOTROSKI_MIN_PASS:
                fail_reasons.append('piotroski_score_3_or_below_chs_fallback')
    else:
        incomplete_reasons.append('missing_altman_model_variant')
    if row.get('_ceo_cfo_self_sale_within_90d') is True:
        fail_reasons.append('ceo_cfo_self_sale_within_90d')
    if row.get('_spac_flag') is True:
        fail_reasons.append('spac_or_no_operating_business')
    if row.get('sec_secondary_offering_flag') is None:
        incomplete_reasons.append('missing_sec_secondary_offering_flag')
    elif row.get('sec_secondary_offering_flag') is True:
        fail_reasons.append('insider_buy_followed_by_secondary_offering')
    if row.get('chinese_adr_flag') is True:
        dual_listing_ok = row.get('dual_listing_flag') is True
        piot_ok = piotroski_score is not None and float(piotroski_score) >= 8.0
        fcf_top_ok = row.get('_fcf_yield_top_10_pct_sector') is True
        if row.get('dual_listing_flag') is None or row.get('_fcf_yield_top_10_pct_sector') is None or piotroski_score is None:
            incomplete_reasons.append('missing_chinese_adr_exception_inputs')
        elif not (dual_listing_ok or (piot_ok and fcf_top_ok)):
            fail_reasons.append('chinese_adr_exception_not_met')
    market_cap_tier = row.get('market_cap_tier')
    analyst_count = row.get('analyst_count')
    if market_cap_tier == 'Mid':
        if analyst_count is None:
            incomplete_reasons.append('missing_mid_tier_analyst_count')
        elif int(analyst_count) < 2:
            fail_reasons.append('mid_tier_analyst_coverage_below_minimum')
    adtv_20d_dollar = row.get('adtv_20d_dollar')
    adtv_min = MIN_ADTV_BY_TIER.get(str(market_cap_tier)) if market_cap_tier else None
    if TOTAL_PORTFOLIO_CAPITAL is not None and market_cap_tier is not None:
        max_pos_pct = _max_position_pct_from_market_cap_tier(market_cap_tier)
        if max_pos_pct is not None:
            adtv_min = max(adtv_min or 0.0, 20.0 * float(TOTAL_PORTFOLIO_CAPITAL) * (float(max_pos_pct) / 100.0))
    if market_cap_tier in {'Large', 'Mid', 'Small'}:
        if adtv_min is None:
            incomplete_reasons.append(f'missing_adtv_threshold_for_{market_cap_tier.lower()}_tier')
        elif adtv_20d_dollar is None:
            incomplete_reasons.append('missing_adtv_20d_dollar')
        elif float(adtv_20d_dollar) < float(adtv_min):
            fail_reasons.append('adtv_below_tier_minimum')
    bid_ask_spread_pct = row.get('bid_ask_spread_pct')
    spread_max = MAX_BID_ASK_SPREAD_PCT_BY_TIER.get(str(market_cap_tier)) if market_cap_tier else None
    if market_cap_tier in {'Large', 'Mid', 'Small'}:
        if spread_max is None:
            incomplete_reasons.append(f'missing_bid_ask_threshold_for_{market_cap_tier.lower()}_tier')
        elif bid_ask_spread_pct is None:
            incomplete_reasons.append('missing_bid_ask_spread_pct')
        elif float(bid_ask_spread_pct) > float(spread_max):
            fail_reasons.append('bid_ask_spread_above_tier_maximum')
    days_to_cover = row.get('days_to_cover')
    if days_to_cover is None:
        incomplete_reasons.append('missing_days_to_cover')
    elif float(days_to_cover) > MAX_DTC_HARD_FAIL:
        fail_reasons.append('days_to_cover_above_15')
    corr = row.get('_correlation_to_top3_120d', CURRENT_CORRELATION_TO_TOP3_120D)
    if corr is not None and float(corr) > 0.65:
        fail_reasons.append('correlation_above_0_65_to_top3')
    if fail_reasons:
        return ('FAIL', fail_reasons)
    if incomplete_reasons:
        return ('INCOMPLETE', incomplete_reasons)
    return ('PASS', [])

def score_factor_1(row: dict[str, Any]) -> dict[str, Any]:
    pct_vs_200 = row.get('pct_vs_200sma')
    quintile = row.get('_momentum_quintile')
    sector_above = row.get('sector_etf_above_200sma')
    peer_method = row.get('_momentum_peer_method')
    golden = row.get('golden_cross_flag')
    death = row.get('death_cross_flag')
    breakout = row.get('breakout_volume_ratio')
    updown = row.get('up_day_volume_ratio_20d')
    hv30 = row.get('hv30_annualized')
    sector_hv_median = row.get('sector_hv_median')
    bear_rally = row.get('bear_rally_constraint_active')
    high_52w_date = row.get('high_52w_date')
    price_as_of = row.get('price_as_of')
    quintile_unavailable_fallback = peer_method in {'sector_etf_relative', 'russell_2000_relative'}
    if pct_vs_200 is None:
        return _factor_result(None, None, 0.25, 'f1_momentum', 'f1_weighted')
    p = float(pct_vs_200)
    raw = None
    if p > 15:
        if quintile_unavailable_fallback:
            raw = 4.0
        elif quintile is None or sector_above is None:
            raw = None
        elif int(quintile) == 5 and sector_above is True:
            raw = 10.0
        elif int(quintile) >= 4 and sector_above is True:
            raw = 8.0
        elif int(quintile) >= 3 and sector_above is True:
            raw = 6.0
        else:
            raw = 4.0
    elif p > 10:
        if quintile_unavailable_fallback:
            raw = 4.0
        elif quintile is None or sector_above is None:
            raw = None
        elif int(quintile) >= 4 and sector_above is True:
            raw = 8.0
        elif int(quintile) >= 3 and sector_above is True:
            raw = 6.0
        else:
            raw = 4.0
    elif p > 5:
        if quintile_unavailable_fallback:
            raw = 4.0
        elif quintile is None or sector_above is None:
            raw = None
        elif int(quintile) >= 3 and sector_above is True:
            raw = 6.0
        else:
            raw = 4.0
    elif p >= 0:
        raw = 4.0
    elif p >= -15:
        raw = 2.0
    elif p >= -30:
        raw = 1.0
    if raw is None:
        return _factor_result(None, None, 0.25, 'f1_momentum', 'f1_weighted')
    modifiers = 0.0
    if sector_above is False:
        modifiers -= 1.0
    if golden is True:
        modifiers += 0.5
    if death is True:
        modifiers -= 0.5
    if breakout is not None and float(breakout) > 1.5:
        modifiers += 0.5
    if updown is not None:
        modifiers += 0.5 if float(updown) > 1.0 else -0.5 if float(updown) < 1.0 else 0.0
    if hv30 is not None and sector_hv_median is not None and (float(sector_hv_median) > 0):
        if float(hv30) < float(sector_hv_median):
            modifiers += 0.5
        elif float(hv30) > 2.0 * float(sector_hv_median):
            modifiers -= 0.5
    adjusted = _clamp_score(raw + modifiers)
    if _bool(bear_rally) and adjusted is not None and (adjusted > 6):
        try:
            if high_52w_date is None or price_as_of is None or (datetime.strptime(price_as_of, '%Y-%m-%d').date() - datetime.strptime(high_52w_date, '%Y-%m-%d').date()).days > 122:
                adjusted = 6.0
        except Exception:
            adjusted = 6.0
    if row.get('hy_oas_bear_rally_active') is True and adjusted is not None:
        adjusted = _clamp_score(adjusted - 5.0)
    return _factor_result(raw, adjusted, 0.25, 'f1_momentum', 'f1_weighted')
def score_factor_5(row: dict[str, Any]) -> dict[str, Any]:
    title = row.get('latest_insider_title')
    signal_age_days = row.get('signal_age_days')
    latest_value = row.get('latest_insider_value')
    holdings_pct = row.get('insider_holdings_pct')
    holdings_verified = row.get('insider_holdings_verified')
    fallback_used = row.get('insider_fallback_threshold_used')
    open_market = row.get('sec_open_market_confirmed')
    tenb5_regime = row.get('sec_10b5_1_regime')
    cluster_flag = row.get('cluster_buy_flag')
    cluster_open_market_best_available = row.get('_cluster_open_market_best_available')
    cluster_count = row.get('cluster_buy_count')
    analyst_count = row.get('analyst_count')
    short_pct = row.get('si_pct_at_filing')
    dtc_check = row.get('dtc_independence_check')
    buy_within_earnings = row.get('buy_within_10d_earnings_flag')
    options_flow = row.get('options_flow_flag')
    si_trending_down = row.get('si_trending_down_flag')
    ttm_buy_vs_dilution = row.get('ttm_insider_buy_vs_dilution')
    sector_underperf = row.get('sector_underperformance_24m')
    ceo_cfo_drawdown = row.get('ceo_cfo_drawdown_buy_flag')
    market_cap_tier = row.get('market_cap_tier')
    cluster_includes_ceo_cfo = row.get('_cluster_includes_ceo_cfo')
    cluster_director_only = row.get('_cluster_director_only')
    roic_top_quartile = row.get('_roic_top_quartile')
    institutional_adj = row.get('_institutional_convergence_adjustment')
    confirmed_catalyst = row.get('_confirmed_catalyst_flag')
    is_swing_trade = row.get('_is_swing_trade')
    raw = None
    is_ceo_cfo = _role_is_ceo_cfo(title)
    is_director = _role_is_director(title)
    director_threshold = _factor5_director_fallback_threshold(market_cap_tier)
    cluster_open_market = open_market is True or (open_market is not False and cluster_open_market_best_available is True)
    if cluster_flag is True and cluster_count is not None and (int(cluster_count) >= 3) and (cluster_includes_ceo_cfo is True) and cluster_open_market and (tenb5_regime is None):
        raw = 10.0
    elif cluster_flag is True and cluster_count is not None and (int(cluster_count) >= 2) and (cluster_includes_ceo_cfo is True) and cluster_open_market:
        raw = 9.0
    elif ceo_cfo_drawdown is True and open_market is True and (roic_top_quartile is True) and (holdings_pct is not None) and (float(holdings_pct) >= 1.0):
        raw = 9.0
    elif cluster_flag is True and cluster_count is not None and (int(cluster_count) >= 2) and (cluster_director_only is True) and cluster_open_market:
        raw = 7.0
    elif is_ceo_cfo and open_market is True and (holdings_pct is not None) and (float(holdings_pct) >= 1.0) and (signal_age_days is not None) and (int(signal_age_days) <= 120):
        raw = 6.0
    elif is_ceo_cfo and market_cap_tier == 'Small' and (open_market is True) and (latest_value is not None) and (float(latest_value) >= 100000) and (signal_age_days is not None) and (int(signal_age_days) <= 120):
        raw = 5.0
    elif is_director and open_market is True and (holdings_pct is not None) and (float(holdings_pct) >= 1.0) and (signal_age_days is not None) and (int(signal_age_days) <= 120):
        raw = 4.0
    elif is_director and open_market is True and (holdings_verified is False) and (fallback_used is True) and (signal_age_days is not None) and (int(signal_age_days) <= 120) and (director_threshold is not None) and (latest_value is not None) and (float(latest_value) >= float(director_threshold)):
        raw = 3.0
    else:
        raw = 0.0
    adjusted = raw
    if signal_age_days is not None:
        if int(signal_age_days) > 240:
            adjusted = 0.0
        elif 120 <= int(signal_age_days) <= 240:
            adjusted -= 2.0
    if tenb5_regime == 'pre_reform_or_unverifiable':
        adjusted -= 3.0
        adjusted = min(adjusted, 4.0)
    elif tenb5_regime == 'post_reform_confirmed':
        adjusted -= 1.0
    if holdings_verified is False and fallback_used is True:
        adjusted -= 1.0
    if short_pct is not None and float(short_pct) > 20.0 and (cluster_flag is not True):
        if market_cap_tier == 'Small' and isinstance(dtc_check, str) and (dtc_check.strip().lower() == 'waived'):
            adjusted -= 1.0
        else:
            adjusted -= 2.0
    if buy_within_earnings is True:
        adjusted -= 1.0
    if institutional_adj is not None:
        adjusted += float(institutional_adj)
    if options_flow is True:
        adjusted += 0.5
    if _bool(is_swing_trade) and short_pct is not None and (float(short_pct) > 10.0) and (confirmed_catalyst is True) and (si_trending_down is True):
        adjusted += 1.0
    elif si_trending_down is True:
        adjusted += 0.5
    if ttm_buy_vs_dilution is not None and float(ttm_buy_vs_dilution) < 0.1 and (row.get('net_dilution_pct') is not None) and (float(row['net_dilution_pct']) > 3.0):
        adjusted -= 2.0
    if analyst_count is not None and int(analyst_count) < 3 and (cluster_flag is True) and (cluster_includes_ceo_cfo is True) and (open_market is True):
        adjusted += 0.5
    adjusted = _clamp_score(adjusted)
    if sector_underperf is not None and float(sector_underperf) <= -15.0:
        adjusted -= 3.0
        if raw > 0:
            adjusted = max(adjusted, 1.0)
    return _factor_result(raw, _clamp_score(adjusted), 0.15, 'f5_insider', 'f5_weighted')
def _apply_composite_core(row: dict[str, Any]) -> dict[str, Any]:
    out = _none_dict(['raw_composite', 'standalone_deduction_vol', 'standalone_deduction_si_dtc', 'standalone_deduction_eq', 'standalone_deduction_concentration', 'standalone_deduction_rates', 'total_standalone_deductions', 'regime_deduction_soft_crash', 'regime_deduction_full_crash', 'regime_deduction_mtum_vtv', 'regime_deduction_hy_oas', 'total_regime_deductions', 'final_composite', 'momentum_dependent_entry_flag', 'zeroed_momentum_composite', 'override_1_holdings', 'override_2_mtum_vtv', 'override_3_soft_crash_insider', 'override_4a_full_crash', 'override_4b_soft_crash_price', 'override_5_below_200sma', 'mid_tier_cap_applied', 'small_tier_cap_applied', 'final_tier', 'tier_floor_pct', 'tier_range_pct', 'composite_minus_floor', 'tier_score_range', 'composite_proportional_size', 'atr_stop_implied_pct', 'atr_adjusted_size', 'final_position_size_pct'])
    weighted_values = [row.get('f1_weighted'), row.get('f2_weighted'), row.get('f3_weighted'), row.get('f4_weighted'), row.get('f5_weighted'), row.get('f6_weighted'), row.get('f7_weighted')]
    if any(v is None for v in weighted_values):
        return out
    out['raw_composite'] = sum(float(v) for v in weighted_values) * 10.0
    hv30 = row.get('hv30_annualized')
    mtum_gt_5 = row.get('_mtum_outperforms_vtv_gt_5pp')
    out['standalone_deduction_vol'] = 0.0 if hv30 is None or mtum_gt_5 is True else STANDALONE_VOL_DEDUCTION if float(hv30) > 40.0 else 0.0
    short_pct = row.get('si_pct_at_filing')
    dtc = row.get('days_to_cover')
    out['standalone_deduction_si_dtc'] = STANDALONE_SI_DTC_DEDUCTION if short_pct is not None and dtc is not None and 5.0 <= float(short_pct) <= 15.0 and 3.0 <= float(dtc) <= 7.0 else 0.0
    eq_score = row.get('stacking_eq_cumulative_score')
    out['standalone_deduction_eq'] = STANDALONE_EQ_DEDUCTION if eq_score is not None and float(eq_score) <= -2.0 else 0.0
    sector_theme_pct = row.get('_sector_theme_portfolio_pct')
    out['standalone_deduction_concentration'] = STANDALONE_CONCENTRATION_DEDUCTION if sector_theme_pct is not None and 20.0 <= float(sector_theme_pct) <= 25.0 else 0.0
    treasury_change = row.get('treasury_10y_30d_change_bps')
    peg = row.get('peg_ratio')
    ps = row.get('price_to_sales')
    rates_trigger = treasury_change is not None and float(treasury_change) > 50.0 and ((ps is not None and float(ps) > 10.0) or (peg is not None and float(peg) > 2.5))
    out['standalone_deduction_rates'] = STANDALONE_RATES_DEDUCTION if rates_trigger else 0.0
    out['total_standalone_deductions'] = sum(float(out[k]) for k in ['standalone_deduction_vol', 'standalone_deduction_si_dtc', 'standalone_deduction_eq', 'standalone_deduction_concentration', 'standalone_deduction_rates'])
    out['regime_deduction_soft_crash'] = REGIME_SOFT_CRASH_DEDUCTION if row.get('soft_crash_flag_active') is True else 0.0
    out['regime_deduction_full_crash'] = REGIME_FULL_CRASH_DEDUCTION if row.get('full_crash_flag_active') is True else 0.0
    out['regime_deduction_mtum_vtv'] = REGIME_MTUM_VTV_DEDUCTION if row.get('mtum_vtv_cap_active') is True else 0.0
    out['regime_deduction_hy_oas'] = 0.0
    out['total_regime_deductions'] = sum(float(out[k]) for k in ['regime_deduction_soft_crash', 'regime_deduction_full_crash', 'regime_deduction_mtum_vtv', 'regime_deduction_hy_oas'])
    out['final_composite'] = float(out['raw_composite']) + float(out['total_standalone_deductions']) + float(out['total_regime_deductions'])
    base_tier = _tier_from_composite(out['final_composite'])
    f1_weighted = row.get('f1_weighted')
    out['zeroed_momentum_composite'] = None if f1_weighted is None else float(out['final_composite']) - float(f1_weighted) * 10.0
    tier_floor_score = _tier_floor_score(base_tier)
    out['momentum_dependent_entry_flag'] = bool(base_tier in {'A', 'B', 'C'} and out['zeroed_momentum_composite'] is not None and tier_floor_score is not None and float(out['zeroed_momentum_composite']) < float(tier_floor_score))
    out['override_1_holdings'] = row.get('insider_holdings_verified') is False and row.get('insider_fallback_threshold_used') is True
    out['override_2_mtum_vtv'] = row.get('mtum_vtv_cap_active') is True and out['momentum_dependent_entry_flag'] is True and out['final_composite'] is not None and float(out['final_composite']) > OVERRIDE_2_CAP_SCORE
    if out['override_2_mtum_vtv']:
        out['final_composite'] = OVERRIDE_2_CAP_SCORE
    final_tier = _tier_from_composite(out['final_composite'])
    out['override_3_soft_crash_insider'] = row.get('soft_crash_flag_active') is True and row.get('f5_insider_raw') is not None and float(row.get('f5_insider_raw')) < 7.0
    within_5 = _within_pct_of_52w_high(row.get('current_price'), row.get('high_52w'), 5.0)
    within_10 = _within_pct_of_52w_high(row.get('current_price'), row.get('high_52w'), 10.0)
    top10 = row.get('_top_10pct_sector_momentum') is True
    top20 = row.get('_top_20pct_sector_momentum') is True
    out['override_4a_full_crash'] = row.get('full_crash_flag_active') is True and within_5 is False and not top10
    out['override_4b_soft_crash_price'] = row.get('soft_crash_flag_active') is True and within_10 is False and not top20
    out['override_5_below_200sma'] = row.get('pct_vs_200sma') is not None and float(row.get('pct_vs_200sma')) < 0 and final_tier in {'A', 'B'}
    if out['override_4a_full_crash']:
        final_tier = 'Eliminate'
    else:
        if out['override_1_holdings'] or out['override_3_soft_crash_insider'] or out['override_4b_soft_crash_price'] or out['override_5_below_200sma']:
            final_tier = _cap_tier_to_c_if_above(final_tier)
    out['mid_tier_cap_applied'] = row.get('market_cap_tier') == 'Mid' and final_tier in {'A', 'B'}
    if out['mid_tier_cap_applied']:
        final_tier = 'C'
    out['small_tier_cap_applied'] = row.get('market_cap_tier') == 'Small' and final_tier in {'A', 'B'}
    if row.get('market_cap_tier') == 'Small' and final_tier in {'A', 'B'}:
        final_tier = 'C'
    out['final_tier'] = final_tier
    out['tier_floor_pct'] = _tier_floor_pct(final_tier)
    out['tier_range_pct'] = _tier_range_pct(final_tier)
    out['tier_score_range'] = _tier_score_range(final_tier)
    score_floor = _tier_floor_score(final_tier)
    out['composite_minus_floor'] = None if out['final_composite'] is None or score_floor is None else float(out['final_composite']) - float(score_floor)
    if final_tier in {'A', 'B', 'C'} and out['final_composite'] is not None and score_floor is not None and out['tier_floor_pct'] is not None and out['tier_range_pct'] is not None and out['tier_score_range'] not in (None, 0):
        size = float(out['tier_floor_pct']) + float(out['tier_range_pct']) * ((float(out['final_composite']) - float(score_floor)) / float(out['tier_score_range']))
        abs_cap = _tier_abs_cap(final_tier)
        if abs_cap is not None:
            size = min(size, float(abs_cap))
        if row.get('market_cap_tier') == 'Small':
            size = min(size, SMALL_TIER_HARD_CAP)
        out['composite_proportional_size'] = max(0.0, size)
    else:
        out['composite_proportional_size'] = None
    atr_20d = row.get('atr_20d')
    current_price = row.get('current_price')
    max_stop_pct = _max_stop_pct_from_trade_type(row.get('_trade_type'))
    if atr_20d is not None and current_price not in (None, 0):
        out['atr_stop_implied_pct'] = float(atr_20d) * 2.5 / float(current_price) * 100.0
    if out['composite_proportional_size'] is not None:
        if out['atr_stop_implied_pct'] is not None and max_stop_pct is not None and float(out['atr_stop_implied_pct']) > float(max_stop_pct):
            out['atr_adjusted_size'] = float(out['composite_proportional_size']) * (float(max_stop_pct) / float(out['atr_stop_implied_pct']))
        else:
            out['atr_adjusted_size'] = float(out['composite_proportional_size'])
        out['final_position_size_pct'] = out['atr_adjusted_size']
        if row.get('market_cap_tier') == 'Small' and out['final_position_size_pct'] is not None:
            out['final_position_size_pct'] = min(float(out['final_position_size_pct']), SMALL_TIER_HARD_CAP)
    return out


def apply_composite(row: dict[str, Any]) -> dict[str, Any]:
    out = _apply_composite_core(row)
    if TOTAL_PORTFOLIO_CAPITAL is not None and out.get('final_position_size_pct') is not None and (row.get('adtv_20d_dollar') not in (None, 0)):
        intended_position_dollar = float(TOTAL_PORTFOLIO_CAPITAL) * (float(out['final_position_size_pct']) / 100.0)
        row['position_to_adtv_pct'] = intended_position_dollar / float(row['adtv_20d_dollar']) * 100.0
    return out


def apply_entry_portfolio_guards(row: dict[str, Any]) -> None:
    return None


def score_and_finalize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enrich_cross_section(rows, CURRENT_PEER_UNIVERSE_ROWS)
    for row in rows:
        hard_result, hard_reasons = apply_hard_floors(row)
        row['hard_floor_result'] = hard_result
        row['hard_floor_fail_reasons'] = '|'.join(hard_reasons)
        f1 = score_factor_1(row)
        _merge_row(row, f1)
        f2 = score_factor_2(row)
        _merge_row(row, {k: v for k, v in f2.items() if k in OUTPUT_COLUMNS})
        row['_stacking_eq_cumulative_score'] = f2.get('_stacking_eq_cumulative_score')
        row['stacking_eq_cumulative_score'] = f2.get('_stacking_eq_cumulative_score')
        f3 = score_factor_3(row)
        _merge_row(row, f3)
        f4 = score_factor_4(row)
        _merge_row(row, f4)
        f5 = score_factor_5(row)
        _merge_row(row, f5)
        f6 = score_factor_6(row)
        _merge_row(row, f6)
        f7 = score_factor_7(row)
        _merge_row(row, f7)
        stacking_result, stacking_fields = apply_stacking_rule(row)
        _merge_row(row, stacking_fields)
        row['stacking_result'] = stacking_result
        ccg_result, ccg_fields = apply_ccg(row)
        _merge_row(row, ccg_fields)
        row['ccg_result'] = ccg_result
        if hard_result == 'PASS' and stacking_result == 'PASS' and ccg_result == 'PASS':
            composite_fields = apply_composite(row)
            _merge_row(row, composite_fields)
            apply_entry_portfolio_guards(row)
        audit = parse_audit_section(row.get('run_id') or '', row.get('run_mode') or '', row.get('source_market_data') or 'alpaca_massive', row.get('source_quote') or 'alpaca_or_polygon', row.get('source_profile') or 'fmp_profile', row.get('source_cashflow') or 'fmp_cashflow', row.get('source_scores') or 'fmp_scores', row.get('source_analyst') or 'finnhub_fmp', row.get('source_insider') or 'finnhub_sec', row.get('source_sec') or 'sec_edgar', row)
        _merge_row(row, audit)
    return rows

def collect_one_ticker(ticker: str, list_source: str, run_id: str, run_mode: str) -> dict[str, Any]:
    row = {col: None for col in OUTPUT_COLUMNS}
    row['ticker'] = ticker
    row['list_source'] = list_source
    start_date, end_date = _start_end_for_daily_bars()
    profile = _safe_call(fmp_get_profile, ticker)
    core = _safe_call(fmp_get_company_core_information, ticker)
    details = _safe_call(massive_get_ticker_details, ticker)
    identity = parse_identity_section(ticker, list_source, profile, details)
    _merge_row(row, identity)
    sector_etf_ticker = _sector_etf_for_sector(identity.get('sector'))
    stock_bars_alpaca = _safe_call(alpaca_get_stock_bars, [ticker], start_date, end_date) if ALPACA_API_KEY and 'YOUR_' not in ALPACA_API_KEY else None
    stock_bars_massive = _safe_call(massive_get_aggregates, ticker, start_date, end_date)
    latest_bars = _safe_call(alpaca_get_latest_bars, [ticker]) if ALPACA_API_KEY and 'YOUR_' not in ALPACA_API_KEY else None
    latest_quotes = _safe_call(alpaca_get_latest_quotes, [ticker]) if ALPACA_API_KEY and 'YOUR_' not in ALPACA_API_KEY else None
    snapshot = _safe_call(massive_get_snapshot, ticker)
    last_trade = _safe_call(massive_get_last_trade, ticker)
    last_quote = _safe_call(massive_get_last_quote, ticker)
    sector_bars_alpaca = _safe_call(alpaca_get_stock_bars, [sector_etf_ticker], start_date, end_date) if sector_etf_ticker and ALPACA_API_KEY and ('YOUR_' not in ALPACA_API_KEY) else None
    sector_bars_massive = _safe_call(massive_get_aggregates, sector_etf_ticker, start_date, end_date) if sector_etf_ticker else None
    price = parse_price_and_trend_section(stock_bars_alpaca or {}, latest_bars or {}, latest_quotes or {}, stock_bars_massive or {}, snapshot or {}, last_trade or {}, last_quote or {}, ticker, sector_etf_ticker, sector_bars_alpaca or {}, sector_bars_massive or {}, None)
    _merge_row(row, price)
    raw_bars = _select_preferred_bar_series(stock_bars_alpaca or {}, stock_bars_massive or {}, ticker, 273)
    highs = [_extract_high_from_bar(b) for b in raw_bars]
    lows = [_extract_low_from_bar(b) for b in raw_bars]
    closes = [_extract_close_from_bar(b) for b in raw_bars]
    row['atr_20d'] = _compute_atr20(highs, lows, closes)
    sector_raw_bars = _select_preferred_bar_series(sector_bars_alpaca or {}, sector_bars_massive or {}, sector_etf_ticker, 200) if sector_etf_ticker else []
    russell_bars_alpaca = _safe_call(alpaca_get_stock_bars, [RUSSELL_2000_PROXY_TICKER], start_date, end_date) if ALPACA_API_KEY and ('YOUR_' not in ALPACA_API_KEY) else None
    russell_bars_massive = _safe_call(massive_get_aggregates, RUSSELL_2000_PROXY_TICKER, start_date, end_date)
    russell_raw_bars = _select_preferred_bar_series(russell_bars_alpaca or {}, russell_bars_massive or {}, RUSSELL_2000_PROXY_TICKER, 200)
    metrics = _safe_call(finnhub_get_metrics, ticker)
    short_interest = _safe_call(massive_get_short_interest, ticker)
    _merge_row(row, parse_liquidity_and_short_section(short_interest or {}, metrics or {}, row.get('adtv_20d_dollar')))
    earnings_surprises_fh = _safe_call(finnhub_get_earnings_surprises, ticker)
    earnings_surprises_fmp = _safe_call(fmp_get_earnings_surprises, ticker)
    earnings_report_fmp = _safe_call(fmp_get_earnings_report, ticker)
    earnings_cal = _safe_call(finnhub_get_earnings_calendar, ticker, (date.today() - timedelta(days=180)).isoformat(), date.today().isoformat())
    analyst_estimates = _safe_call(fmp_get_analyst_estimates, ticker)
    rev_source = _build_revision_source_from_fmp_analyst_estimates(analyst_estimates, ticker)
    last_earn_dt = _extract_last_earnings_date_from_payloads(earnings_surprises_fh or [], earnings_surprises_fmp or [], earnings_report_fmp or [], earnings_cal or {})
    ticker_3d = _compute_3d_post_earnings_return_from_bars(raw_bars, last_earn_dt)
    sector_3d = _compute_3d_post_earnings_return_from_bars(sector_raw_bars, last_earn_dt) if sector_etf_ticker else None
    _merge_row(row, parse_earnings_and_ear_section(earnings_surprises_fh or [], earnings_cal or {}, analyst_estimates or [], earnings_surprises_fmp or [], earnings_report_fmp or [], rev_source, ticker_3d, sector_3d, row.get('market_cap_tier')))
    row['trading_days_since_earnings'] = _exact_trading_days_since_from_bars(raw_bars, row.get('last_earnings_date'))
    _expired_exact = row['trading_days_since_earnings'] is not None and row['trading_days_since_earnings'] > 63
    _insuff = row.get('ear_exempt_reason') == 'insufficient_analyst_coverage'
    row['ear_neutral_flag'] = bool(_expired_exact or _insuff)
    row['ear_exempt_reason'] = 'expired_gt_63d' if _expired_exact else 'insufficient_analyst_coverage' if _insuff else None
    km_ttm = _safe_call(fmp_get_key_metrics_ttm, ticker)
    ratios_ttm = _safe_call(fmp_get_ratios_ttm, ticker)
    km_q = _safe_call(fmp_get_key_metrics_quarter, ticker)
    dcf = _safe_call(fmp_get_dcf, ticker)
    cf_annual = _safe_call(fmp_get_cash_flow_annual, ticker)
    cf_ttm = _safe_call(fmp_get_cash_flow_ttm, ticker)
    income_ttm = _safe_call(fmp_get_income_statement_ttm, ticker)
    bal_annual = _safe_call(fmp_get_balance_sheet_annual, ticker)
    sec_cik = _extract_cik_from_fmp(profile or {}, core or {})
    row['sec_cik'] = sec_cik
    bal_q = _safe_call(fmp_get_balance_sheet_quarter, ticker)
    _merge_row(row, parse_profitability_section(km_ttm or [], ratios_ttm or [], km_q or [], dcf or [], cf_annual or [], income_ttm or [], bal_annual or [], bal_q or [], cf_ttm or []))
    inc_q = _safe_call(fmp_get_income_statement_quarter, ticker)
    _merge_row(row, parse_earnings_quality_section(income_ttm or [], cf_ttm or [], bal_annual or [], bal_q or [], inc_q or []))
    ev = _safe_call(fmp_get_enterprise_values, ticker)
    _merge_row(row, parse_valuation_section(ratios_ttm or [], km_ttm or [], ev or [], bal_annual or [], income_ttm or [], analyst_estimates or [], row.get('price_to_sales'), row.get('current_price'), bal_q or []))
    scores = _safe_call(fmp_get_financial_scores, ticker)
    _merge_row(row, parse_solvency_section(scores or [], row.get('altman_model_variant'), row.get('days_to_cover'), ticker))
    sec_submissions = _safe_call(sec_get_company_submissions, sec_cik) if sec_cik else None
    insider_tx = _safe_call(finnhub_get_insider_transactions, ticker, (date.today() - timedelta(days=365)).isoformat(), date.today().isoformat())
    target_form4_date, target_buyer_name = _latest_buy_identity_from_transactions(insider_tx or {})
    form4_url = _latest_form4_url_from_submissions(sec_cik, sec_submissions or {}, target_form4_date, target_buyer_name)
    filing_text = _safe_call(sec_get_filing_text, form4_url) if form4_url else None
    _merge_row(row, parse_insider_section(insider_tx or {}, filing_text, row.get('current_price'), row.get('high_52w')))
    anchor_date = row.get('latest_insider_filing_date') if row.get('signal_age_days') is not None and int(row.get('signal_age_days')) <= 60 else row.get('price_as_of')
    row['_underperf_reference_date'] = anchor_date
    row['_stock_ret_24m_anchor'] = _compute_trailing_return_from_bars_anchored(raw_bars, 504, anchor_date)
    row['_sector_etf_ret_24m_anchor'] = _compute_trailing_return_from_bars_anchored(sector_raw_bars, 504, anchor_date) if sector_etf_ticker else None
    row['_russell2000_return_12_1_skip_month'] = _compute_12_1_skip_month_exact(russell_raw_bars) if russell_raw_bars else None
    row['buy_within_10d_earnings_flag'] = None
    _insider_rows = _get_nested(insider_tx or {}, 'data')
    _insider_rows = _insider_rows if isinstance(_insider_rows, list) else []
    _buy_within_flags = []
    for _r in _insider_rows:
        _code = str(_latest_non_null([_r.get('transactionCode'), _r.get('transactionType')]) or '').upper()
        if _code not in {'P', 'BUY'}:
            continue
        _buy_dt = _parse_iso_date(_latest_non_null([_r.get('transactionDate'), _r.get('filingDate')]))
        _diff = _exact_abs_trading_days_diff_from_bars(raw_bars, row.get('last_earnings_date'), _buy_dt)
        if _diff is not None:
            _buy_within_flags.append(_diff <= 10)
    if _buy_within_flags:
        row['buy_within_10d_earnings_flag'] = any(_buy_within_flags)
    _merge_row(row, parse_sec_filing_section(sec_cik, sec_submissions or {}, row.get('latest_insider_filing_date')))
    _merge_row(row, parse_manual_review_section(identity, row, row, row))
    # Audit section deliberately omitted here — null_fields must be computed on the final
    # fully-merged row in score_and_finalize_rows(), not on the partial collection row.
    return row

if __name__ == '__main__':
    main()
