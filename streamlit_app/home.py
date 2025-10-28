# ./streamlit_app/Home.py

import streamlit as st
import pandas as pd
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


st.set_page_config(
    page_title="Retail Data Platform",
    page_icon="#",
    layout="wide",
)

# Custom CSS for black & neon blue theme
st.markdown(
    """
    <style>
        /* Background and text styling */
        .stApp {
            background-color: #0b0c10;
            color: #ffffff;
            font-family: 'Inter', sans-serif;
        }

        h1, h2, h3, h4 {
            color: #00b4d8;
            font-weight: 700;
        }

        /* KPI Cards */
        .metric-card {
            background: linear-gradient(145deg, #0d1b2a, #1b263b);
            border: 1px solid #00b4d8;
            border-radius: 18px;
            padding: 1.2rem;
            text-align: center;
            box-shadow: 0px 0px 20px #00b4d820;
        }

        /* Tagline Animation */
        .tagline {
            color: #00b4d8;
            font-size: 1.2rem;
            letter-spacing: 1px;
            text-align: center;
            font-style: italic;
            animation: flicker 3s infinite;
        }

        @keyframes flicker {
            0%, 19%, 21%, 23%, 25%, 54%, 56%, 100% {
                opacity: 1;
            }
            20%, 24%, 55% {
                opacity: 0.3;
            }
        }

        /* Divider */
        .divider {
            height: 1px;
            background-color: #00b4d840;
            margin: 1.5rem 0;
        }
    </style>
    """,
    unsafe_allow_html=True
)

# ------------------------------
# ⚙️ SUPABASE CONNECTION
# ------------------------------
# Replace with your Supabase credentials
SUPABASE_URL = st.secrets["SUPABASE_URL"]
SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ------------------------------
# 📦 LOAD DATA FROM MART
# ------------------------------
@st.cache_data(ttl=300)
def load_kpis():
    try:
        response = supabase.table("mart_sales_kpis").select("*").execute()
        df = pd.DataFrame(response.data)
        return df
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return pd.DataFrame()

df = load_kpis()

# ------------------------------
# 🏠 HERO SECTION
# ------------------------------
st.markdown("<h1 style='text-align:center;'>Retail Data Platform 💡</h1>", unsafe_allow_html=True)
st.markdown("<p class='tagline'>I make data dance — insights in minutes, not hours ⚡</p>", unsafe_allow_html=True)
st.markdown("<div class='divider'></div>", unsafe_allow_html=True)

# ------------------------------
# 📊 KPI CARDS
# ------------------------------
if not df.empty:
    total_sales = df['total_revenue'].sum()
    avg_discount = df['avg_discount'].mean()
    total_transactions = df['total_transactions'].sum()
    last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <h3>💰 Total Revenue</h3>
            <h2>${total_sales:,.2f}</h2>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <h3>🛍️ Total Transactions</h3>
            <h2>{int(total_transactions):,}</h2>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <h3>🏷️ Avg Discount</h3>
            <h2>{avg_discount:.2f}%</h2>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
    st.markdown(f"<p style='text-align:center;color:#9be2ff;'>🕒 Data last refreshed: {last_updated}</p>", unsafe_allow_html=True)
else:
    st.warning("⚠️ No KPI data found. Please confirm your mart_sales_kpis table exists in Supabase.")

# ------------------------------
# 🧭 NAVIGATION HINT
# ------------------------------
st.markdown("<div class='divider'></div>", unsafe_allow_html=True)
st.markdown(
    """
    <p style='text-align:center;color:#9be2ff;font-size:1rem;'>
        Explore further ➡️ Check <b>Data Marts Explorer</b> or <b>Analytics Dashboard</b> pages.
    </p>
    """,
    unsafe_allow_html=True
)
