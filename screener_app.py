"""
Mini Screener Momentum - Azioni/ETF (tipo Finviz/TradingView)
Universi: S&P 500, Nasdaq 100 da CSV locali (aggiornati automaticamente)
Filtri: prezzo, volume, ROC, performance %, preset momentum
"""

import io
import os
import streamlit as st
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime, timedelta
from pathlib import Path 

DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

@st.cache_data(ttl=3600)  # cache 1h per dati storici
def get_history(symbols, period="6mo", interval="1d"):
    data = yf.download(
        " ".join(symbols),
        period=period,
        interval=interval,
        auto_adjust=True,
        progress=False,
        threads=True,
    )
    if len(symbols) == 1:
        data.columns = pd.MultiIndex.from_product([symbols, data.columns])
    return data

def compute_momentum(df_hist, lookback_roc=60, lookback_perf=20):
    closes = df_hist.xs("Close", axis=1, level=1)
    volumes = df_hist.xs("Volume", axis=1, level=1)

    latest_close = closes.iloc[-1]
    latest_vol = volumes.tail(20).mean()

    if len(closes) > lookback_roc:
        past_close_roc = closes.iloc[-lookback_roc]
        roc = (latest_close - past_close_roc) / past_close_roc * 100.0
    else:
        roc = pd.Series(index=latest_close.index, dtype=float)

    if len(closes) > lookback_perf:
        past_close_perf = closes.iloc[-lookback_perf]
        perf = (latest_close - past_close_perf) / past_close_perf * 100.0
    else:
        perf = pd.Series(index=latest_close.index, dtype=float)

    mom = pd.DataFrame({
        "close": latest_close,
        "volume_avg20": latest_vol,
        f"roc_{lookback_roc}": roc,
        f"perf_{lookback_perf}": perf,
    })
    return mom

def download_sp500():
    try:
        url = "https://datahub.io/core/s-and-p-500-companies/r/0.csv"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        df_raw = pd.read_csv(io.StringIO(resp.text))
        
        df = pd.DataFrame({
            "symbol": df_raw["Symbol"].str.strip(),
            "name": df_raw["Security"].str.strip(),
            "sector": df_raw["GICS Sector"].str.strip(),
        })
        df["type"] = "Stock"
        df["universe"] = "S&P 500"
        df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        return df
    except Exception as e:
        st.error(f"Errore download S&P 500: {e}")
        return pd.DataFrame()

def download_nasdaq100():
    try:
        # Fonte più stabile per Nasdaq 100
        url = "https://raw.githubusercontent.com/datasets/finance-vix/main/data/nasdaq100.csv"
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text))
        
        # Pulizia e standardizzazione
        df["symbol"] = df["Symbol"].str.strip().str.upper()
        df["name"] = df["Name"].str.strip()
        df["sector"] = df.get("Sector", "Technology").str.strip()
        
        df = df[["symbol", "name", "sector"]].drop_duplicates(subset="symbol")
        df["type"] = "Stock"
        df["universe"] = "Nasdaq 100"
        df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
        return df
        
    except Exception:
        try:
            # Fallback Wikipedia con gestione errori migliorata
            tables = pd.read_html("https://en.wikipedia.org/wiki/Nasdaq-100", timeout=30)
            for tbl in tables:
                cols = [c.lower() for c in tbl.columns]
                if "ticker" in cols and "company" in cols:
                    symbol_col = next(c for c in tbl.columns if "ticker" in str(c).lower())
                    name_col = next(c for c in tbl.columns if "company" in str(c).lower())
                    sector_col = next((c for c in tbl.columns if any(x in str(c).lower() for x in ["industry", "sector"])), None)
                    
                    df = pd.DataFrame({
                        "symbol": tbl[symbol_col].astype(str).str.strip().str.upper(),
                        "name": tbl[name_col].astype(str).str.strip(),
                    })
                    if sector_col and sector_col in tbl.columns:
                        df["sector"] = tbl[sector_col].astype(str).str.strip()
                    else:
                        df["sector"] = "Technology"
                    
                    df["type"] = "Stock"
                    df["universe"] = "Nasdaq 100"
                    df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
                    return df
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Errore download Nasdaq 100: {e}")
            return pd.DataFrame()

def update_universes():
    """Aggiorna tutti gli universi e salva CSV locali"""
    with st.spinner("Aggiornamento universi in corso..."):
        sp500 = download_sp500()
        nasdaq100 = download_nasdaq100()
        
        if not sp500.empty:
            sp500.to_csv(DATA_DIR / "sp500_constituents.csv", index=False)
        if not nasdaq100.empty:
            nasdaq100.to_csv(DATA_DIR / "nasdaq100_constituents.csv", index=False)
        
        if sp500.empty and nasdaq100.empty:
            st.error("❌ Impossibile aggiornare gli universi")
        else:
            st.success("✅ Universi aggiornati!")
        st.rerun()

def load_universe():
    """Carica universi da CSV locali con fallback a download"""
    sp500_file = DATA_DIR / "sp500_constituents.csv"
    nasdaq100_file = DATA_DIR / "nasdaq100_constituents.csv"
    
    dfs = []
    
    if sp500_file.exists():
        try:
            df_sp = pd.read_csv(sp500_file)
            dfs.append(df_sp)
            if 'updated_at' in df_sp.columns:
                st.sidebar.caption(f"**S&P 500** ultimo update: {df_sp['updated_at'].iloc[0]}")
        except:
            st.sidebar.warning("❌ Errore lettura S&P 500 CSV")
    
    if nasdaq100_file.exists():
        try:
            df_ndx = pd.read_csv(nasdaq100_file)
            dfs.append(df_ndx)
            if 'updated_at' in df_ndx.columns:
                st.sidebar.caption(f"**Nasdaq 100** ultimo update: {df_ndx['updated_at'].iloc[0]}")
        except:
            st.sidebar.warning("❌ Errore lettura Nasdaq 100 CSV")
    
    if not dfs:
        st.sidebar.warning("📥 Nessun file CSV trovato. Usa 'Aggiorna universi'")
        return pd.DataFrame(columns=["symbol","name","sector","type","universe"])
    
    return pd.concat(dfs, ignore_index=True)

def main():
    st.set_page_config(page_title="Screener Momentum", layout="wide")
    st.title("🔍 Dashboard Screener Momentum (Azioni/ETF)")

    # Sidebar: controllo universi + filtri
    st.sidebar.header("🌍 Universi")
    
    if st.sidebar.button("🔄 Aggiorna universi", use_container_width=True):
        update_universes()

    universe = load_universe()
    if universe.empty:
        st.error("❌ Nessun universo disponibile. Clicca 'Aggiorna universi' nella sidebar.")
        st.stop()

    st.sidebar.caption(f"**Totale simboli**: {len(universe)}")

    # Filtri principali
    st.sidebar.header("⚙️ Filtri")
    
    universe_sel = st.sidebar.multiselect(
        "Universo",
        options=sorted(universe["universe"].unique()),
        default=sorted(universe["universe"].unique())
    )
    
    tipo = st.sidebar.multiselect(
        "Tipo",
        options=sorted(universe["type"].unique()),
        default=sorted(universe["type"].unique())
    )
    
    prezzo_min, prezzo_max = st.sidebar.slider(
        "Prezzo ultimo",
        0.0, 2000.0, (0.0, 2000.0)
    )
    
    volume_min = st.sidebar.number_input(
        "Volume medio 20g minimo",
        min_value=0, value=0, step=10000
    )
    
    # Filtri momentum
    st.sidebar.subheader("🚀 Momentum")
    
    roc_lb = st.sidebar.number_input(
        "Lookback ROC (giorni)",
        min_value=10, max_value=252, value=60, step=10
    )
    perf_lb = st.sidebar.number_input(
        "Lookback performance (giorni)",
        min_value=5, max_value=252, value=20, step=5
    )
    
    roc_min = st.sidebar.number_input(
        f"ROC min (% su {roc_lb}g)",
        min_value=-100.0, max_value=500.0, value=-10.0, step=1.0
    )
    perf_min = st.sidebar.number_input(
        f"Perf min (% ultimi {perf_lb}g)",
        min_value=-100.0, max_value=500.0, value=-5.0, step=1.0
    )

    # Barra superiore
    col_top1, col_top2 = st.columns([1, 2])
    with col_top1:
        preset = st.selectbox(
            "Preset momentum",
            options=["Nessuno", "Momentum forte", "Momentum medio", "Rebound"]
        )
    with col_top2:
        search = st.text_input("Cerca simbolo/nome")

    # Filtra universo base
    df_base = universe.copy()
    if universe_sel:
        df_base = df_base[df_base["universe"].isin(universe_sel)]
    if tipo:
        df_base = df_base[df_base["type"].isin(tipo)]
    if search:
        s = search.lower()
        df_base = df_base[
            df_base["symbol"].str.lower().str.contains(s, na=False) |
            df_base["name"].str.lower().str.contains(s, na=False)
        ]

    symbols = df_base["symbol"].tolist()
    if not symbols:
        st.warning("Nessun simbolo nell'universo filtrato.")
        st.stop()

    # Calcola momentum con yfinance
    with st.spinner("📊 Calcolo momentum..."):
        try:
            hist = get_history(symbols, period="12mo", interval="1d")
            mom = compute_momentum(hist, lookback_roc=roc_lb, lookback_perf=perf_lb)
        except Exception as e:
            st.error(f"Errore calcolo momentum: {e}")
            st.stop()

    df = df_base.set_index("symbol").join(mom, how="left")

    # Preset momentum
    if preset == "Momentum forte":
        df = df[(df[f"roc_{roc_lb}"] >= 30) & (df[f"perf_{perf_lb}"] >= 10)]
    elif preset == "Momentum medio":
        df = df[(df[f"roc_{roc_lb}"] >= 10) & (df[f"perf_{perf_lb}"] >= 5)]
    elif preset == "Rebound":
        df = df[(df[f"roc_{roc_lb}"] >= 10) & (df[f"perf_{perf_lb}"] <= 0)]

    # Filtri quantitativi
    mask = (
        (df["close"] >= prezzo_min) &
        (df["close"] <= prezzo_max) &
        (df["volume_avg20"] >= volume_min) &
        (df[f"roc_{roc_lb}"] >= roc_min) &
        (df[f"perf_{perf_lb}"] >= perf_min)
    )
    df = df[mask].dropna(subset=[f"roc_{roc_lb}", f"perf_{perf_lb}"])

    # Ordina per ROC decrescente
    df = df.sort_values(by=f"roc_{roc_lb}", ascending=False)

    # Layout risultati
    left, right = st.columns([2, 3])

    with left:
        st.subheader("📋 Risultati filtrati")
        if df.empty:
            st.info("Nessun titolo soddisfa i filtri.")
        else:
            display_cols = ["name", "universe", "sector", "close", "volume_avg20", 
                           f"roc_{roc_lb}", f"perf_{perf_lb}"]
            st.dataframe(
                df[display_cols].round(2),
                use_container_width=True,
                height=600
            )
            
            csv_data = df.reset_index()[["symbol", "name", "sector", "universe", "type", 
                                       "close", "volume_avg20", f"roc_{roc_lb}", f"perf_{perf_lb}"]].round(2)
            st.download_button(
                label="📥 Download CSV risultati",
                data=csv_data.to_csv(index=False),
                file_name=f"screener_results_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                mime="text/csv"
            )

            if not df.empty:
                selected_symbol = st.selectbox(
                    "Seleziona per dettaglio",
                    options=df.index.tolist(),
                    index=0
                )

    with right:
        st.subheader("📈 Dettaglio")
        if 'selected_symbol' in locals() and selected_symbol and selected_symbol in df.index:
            row = df.loc[selected_symbol]
            st.markdown(f"**{selected_symbol}** – {row['name']} ({row['sector']}, {row['universe']})")
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Prezzo", f"{row['close']:.2f}")
            col2.metric("Vol 20g", f"{row['volume_avg20']:,.0f}")
            col3.metric("ROC", f"{row[f'roc_{roc_lb}']:.2f}%")
            col4.metric("Perf", f"{row[f'perf_{perf_lb}']:.2f}%")
            
            timeframe = st.radio("Timeframe", options=["3M", "6M", "1Y"], horizontal=True)
            period_map = {"3M": "3mo", "6M": "6mo", "1Y": "1y"}
            
            try:
                hist_one = get_history([selected_symbol], period=period_map[timeframe], interval="1d")
                if selected_symbol in hist_one.columns.get_level_values(0):
                    close_series = hist_one[selected_symbol]["Close"].dropna()
                    st.line_chart(close_series)
                else:
                    st.warning("Dati storici non disponibili")
            except:
                st.warning("Errore caricamento grafico")

if __name__ == "__main__":
    main()
