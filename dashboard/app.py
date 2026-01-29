import streamlit as st
import requests
import pandas as pd
import os
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

st.set_page_config(layout="wide")
st_autorefresh(interval=5000, key="datarefresh")

# API URL 설정
API_URL = os.getenv("API_URL", "http://api-server:8000")

# 사이드바 설정
menu = st.sidebar.radio("", ("Insight", "Monitoring"), index=0)

if menu == "Insight":
    st.title("Business Insights")

    # --- 1. 유저별 카테고리 선호도 [입력칸 | 원그래프 | 빈공간] ---
    st.header("User Category Preference")

    # 3개 컬럼 레이아웃 적용
    col_input, col_chart, col_empty = st.columns([1, 2, 1])

    with col_input:
        user_id = st.text_input("Enter User ID", value="517953667")

    if user_id:
        try:
            res = requests.get(f"{API_URL}/user/preference/{user_id}", timeout=2)
            if res.status_code == 200 and res.json():
                pref_data = res.json()
                with col_chart:
                    fig = px.pie(names=[p[0] for p in pref_data], values=[p[1] for p in pref_data],
                                 hole=0.4, color_discrete_sequence=['#71c7d5', '#f3d06a', '#ee9e84'])
                    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=300, showlegend=True)
                    st.plotly_chart(fig, use_container_width=True)
                with col_empty:
                    st.write("")  # 빈 공간 레이아웃
            else:
                st.info("데이터가 없습니다.")
        except:
            st.error("API 연결 실패 (상태를 확인하세요)")

    st.divider()

    # --- 2. 카테고리별 인기 상품 (초기 네모 박스 5개 형태) ---
    st.header("Category Top 5 Products")
    try:
        cat_res = requests.get(f"{API_URL}/categories", timeout=2)
        if cat_res.status_code == 200:
            categories = cat_res.json()
            selected_cat = st.selectbox("Select Category", categories)

            if selected_cat:
                rank_res = requests.get(f"{API_URL}/category/rank/{selected_cat}", timeout=2)
                if rank_res.status_code == 200 and rank_res.json():
                    rank_list = rank_res.json()

                    # 다시 5개의 네모 박스(카드) 형태로 복구
                    box_cols = st.columns(5)
                    for i, item in enumerate(rank_list):
                        with box_cols[i]:
                            st.markdown(f"""
                            <div style="border: 2px solid #000080; border-radius: 10px; padding: 5px; text-align: center; background-color: #f8f9fa; min-height: 100px;">
                                <h4 style="margin:0; color:#333;">TOP {i + 1}</h4>
                                <p style="font-size:20px; color:#666; margin:5px 0;">ID: {item['product_id']}</p>
                                <hr style="margin:10px 0; border:0.5px solid #000080;">
                                <p style="font-size:20px; font-weight:bold; color:#000080;">{item['score']} pts</p>
                            </div>
                            """, unsafe_allow_html=True)
    except:
        st.info("카테고리 목록을 불러오는 중...")