import time
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
import extra_streamlit_components as stx
from streamlit_option_menu import option_menu
from supabase import create_client, Client

# =========================
# 1. CONFIGURATION
# =========================

st.set_page_config(
    page_title="AI Visibility by Virshi",
    page_icon="👁️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 🔴 ПРОДАКШН N8N ВЕБХУКИ
N8N_GEN_URL = "https://virshi.app.n8n.cloud/webhook/webhook/generate-prompts"
N8N_ANALYZE_URL = "https://virshi.app.n8n.cloud/webhook-test/webhook/run-analysis"

# Custom CSS
st.markdown(
    """
<style>
    .stApp { background-color: #F4F6F9; }
    section[data-testid="stSidebar"] { background-color: #FFFFFF; border-right: 1px solid #E0E0E0; }
    .sidebar-logo-container { display: flex; justify-content: center; margin-bottom: 10px; }
    .sidebar-logo-container img { width: 140px; }

    .css-1r6slb0, .css-12oz5g7, div[data-testid="stForm"] {
        background-color: white; padding: 20px; border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05); border: 1px solid #EAEAEA;
    }
    div[data-testid="stMetric"] {
        background-color: #ffffff; border: 1px solid #e0e0e0; padding: 15px;
        border-radius: 10px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }

    .stButton>button { background-color: #8041F6; color: white; border-radius: 8px; border: none; font-weight: 600; }
    .stButton>button:hover { background-color: #6a35cc; }
    .upgrade-btn {
        display: block; width: 100%; background-color: #FFC107; color: #000000;
        text-align: center; padding: 8px; border-radius: 8px;
        text-decoration: none; font-weight: bold; margin-top: 10px; border: 1px solid #e0a800;
    }
    .badge-trial { background-color: #FFECB3; color: #856404; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.7em; }
    .badge-active { background-color: #D4EDDA; color: #155724; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.7em; }

    .sidebar-name { font-size: 14px; font-weight: 600; color: #333; margin-top: 5px;}
    .sidebar-label { font-size: 11px; color: #999; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 15px;}

    /* Таблиці з детальними відповідями */
    .query-detail-box {
        background: #ffffff;
        border-radius: 10px;
        border: 1px solid #EAEAEA;
        padding: 16px;
        margin-bottom: 16px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.03);
    }
</style>
""",
    unsafe_allow_html=True,
)

# =========================
# 2. DB CONNECTION & STATE
# =========================

cookie_manager = stx.CookieManager()

try:
    SUPABASE_URL: str = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY: str = st.secrets["SUPABASE_KEY"]
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    DB_CONNECTED = True
except Exception as e:
    st.error(f"CRITICAL ERROR: Database Connection Failed. {e}")
    st.stop()

# Session State
if "user" not in st.session_state:
    st.session_state["user"] = None
if "user_details" not in st.session_state:
    st.session_state["user_details"] = {}
if "role" not in st.session_state:
    st.session_state["role"] = "user"
if "current_project" not in st.session_state:
    st.session_state["current_project"] = None
if "generated_prompts" not in st.session_state:
    st.session_state["generated_prompts"] = []
if "onboarding_step" not in st.session_state:
    st.session_state["onboarding_step"] = 2
if "requested_page" not in st.session_state:
    st.session_state["requested_page"] = None
if "focus_keyword" not in st.session_state:
    st.session_state["focus_keyword"] = None

# =========================
# 3. HELPERS
# =========================


def get_donut_chart(value, color="#00C896"):
    value = float(value) if value else 0.0
    remaining = max(0, 100 - value)
    fig = go.Figure(
        data=[
            go.Pie(
                values=[value, remaining],
                hole=0.75,
                marker_colors=[color, "#F0F2F6"],
                textinfo="none",
                hoverinfo="label+percent",
            )
        ]
    )
    fig.update_layout(
        showlegend=False,
        margin=dict(t=0, b=0, l=0, r=0),
        height=80,
        width=80,
        annotations=[
            dict(
                text=f"{int(value)}%",
                x=0.5,
                y=0.5,
                font_size=14,
                showarrow=False,
                font_weight="bold",
                font_color="#333",
            )
        ],
    )
    return fig


METRIC_TOOLTIPS = {
    "sov": "Частка видимості вашого бренду у відповідях ШІ порівняно з конкурентами.",
    "official": "Частка посилань на ваші офіційні ресурси.",
    "sentiment": "Тональність: Позитивна, Нейтральна або Негативна.",
    "position": "Середня позиція вашого бренду у списках рекомендацій.",
    "presence": "Відсоток запитів, де бренд був згаданий.",
    "domain": "Відсоток запитів з клікабельним посиланням на ваш домен.",
    "coverage": "Частка запитів, які вже були проаналізовані хоча б однією LLM.",
    "freshness": "Наскільки нещодавно (у днях) оновлювались відповіді LLM.",
}


def n8n_generate_prompts(brand: str, domain: str, industry: str, products: str):
    """Викликає n8n вебхук для генерації промптів."""
    try:
        payload = {
            "brand": brand,
            "domain": domain,
            "industry": industry,
            "products": products,
        }
        response = requests.post(N8N_GEN_URL, json=payload, timeout=20)

        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                return data
            return data.get("prompts", [])
        else:
            st.error(f"N8N Error: {response.status_code} - {response.text}")
            return []
    except Exception as e:
        st.error(f"Помилка з'єднання з N8N: {e}")
        return []


def n8n_trigger_analysis(project_id, keywords, brand_name, llms=None):
    """
    Запуск аналізу для довільної кількості запитів + список LLM.
    n8n сам пише результати в Supabase.
    """
    try:
        user_email = st.session_state["user"].email
        payload = {
            "project_id": project_id,
            "keywords": keywords,
            "brand_name": brand_name,
            "user_email": user_email,
            "llms": llms or [],
        }
        requests.post(N8N_ANALYZE_URL, json=payload, timeout=2)
        return True
    except requests.exceptions.ReadTimeout:
        return True
    except Exception as e:
        st.error(f"Помилка запуску аналізу: {e}")
        return False


# =========================
# 4. AUTH & USER LOGIC
# =========================


def load_user_project(user_id: str) -> bool:
    try:
        res = supabase.table("projects").select("*").eq("user_id", user_id).execute()
        if res.data and len(res.data) > 0:
            st.session_state["current_project"] = res.data[0]
            return True
    except Exception:
        pass
    return False


def get_user_role_and_details(user_id: str):
    try:
        data = supabase.table("profiles").select("*").eq("id", user_id).execute()
        if data.data:
            p = data.data[0]
            return p.get("role", "user"), {
                "first_name": p.get("first_name"),
                "last_name": p.get("last_name"),
            }
    except Exception:
        pass
    return "user", {}


def check_session():
    if st.session_state["user"] is None:
        time.sleep(0.1)
        token = cookie_manager.get("virshi_auth_token")

        if token:
            try:
                res = supabase.auth.get_user(token)
                if res.user:
                    st.session_state["user"] = res.user
                    role, details = get_user_role_and_details(res.user.id)
                    st.session_state["role"] = role
                    st.session_state["user_details"] = details
                    load_user_project(res.user.id)
                else:
                    cookie_manager.delete("virshi_auth_token")
            except Exception:
                cookie_manager.delete("virshi_auth_token")


def login_user(email: str, password: str):
    try:
        res = supabase.auth.sign_in_with_password(
            {"email": email, "password": password}
        )
        if not res.user:
            st.error("Не вдалося увійти. Перевірте email та пароль.")
            return

        st.session_state["user"] = res.user
        cookie_manager.set(
            "virshi_auth_token",
            res.session.access_token,
            expires_at=datetime.now() + timedelta(days=7),
        )

        role, details = get_user_role_and_details(res.user.id)
        st.session_state["role"] = role
        st.session_state["user_details"] = details

        if load_user_project(res.user.id):
            st.success("Вхід успішний!")

        st.rerun()
    except Exception:
        st.error(
            "Помилка входу: невірний логін, пароль або налаштування підтвердження email."
        )


def register_user(email: str, password: str, first: str, last: str) -> bool:
    """Реєстрація нового користувача + запис first_name / last_name."""
    try:
        res = supabase.auth.sign_up(
            {
                "email": email,
                "password": password,
                "options": {"data": {"first_name": first, "last_name": last}},
            }
        )

        if res.user:
            try:
                supabase.table("profiles").insert(
                    {
                        "id": res.user.id,
                        "email": email,
                        "first_name": first,
                        "last_name": last,
                        "role": "user",
                    }
                ).execute()
            except Exception:
                pass

            if res.session:
                st.success("Реєстрація успішна! Виконуємо вхід...")
                st.session_state["user"] = res.user
                cookie_manager.set(
                    "virshi_auth_token",
                    res.session.access_token,
                    expires_at=datetime.now() + timedelta(days=7),
                )
                role, details = get_user_role_and_details(res.user.id)
                st.session_state["role"] = role
                st.session_state["user_details"] = details
                load_user_project(res.user.id)
                st.rerun()
            else:
                st.success(
                    "Реєстрація успішна! Перевірте пошту, підтвердіть email "
                    "та увійдіть на вкладці «Вхід»."
                )
            return True

        st.error("Не вдалося створити користувача. Перевірте налаштування Auth.")
    except Exception as e:
        if "already registered" in str(e):
            st.warning("Користувач вже існує. Спробуйте увійти.")
        else:
            st.error(f"Помилка реєстрації: {e}")
    return False


def logout():
    try:
        supabase.auth.sign_out()
    except Exception:
        pass
    cookie_manager.delete("virshi_auth_token")
    st.session_state["user"] = None
    st.session_state["current_project"] = None
    st.rerun()


def login_page():
    c_l, c_center, c_r = st.columns([1, 1.5, 1])
    with c_center:
        st.markdown(
            '<div style="text-align: center;"><img src="https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png" width="180"></div>',
            unsafe_allow_html=True,
        )
        st.markdown("<br>", unsafe_allow_html=True)

        t1, t2 = st.tabs(["🔑 Вхід", "📝 Реєстрація"])

        # ВХІД
        with t1:
            with st.form("login"):
                email = st.text_input("Емейл")
                password = st.text_input("Пароль", type="password")
                if st.form_submit_button("Увійти", use_container_width=True):
                    if email and password:
                        login_user(email, password)
                    else:
                        st.warning("Введіть емейл та пароль.")

        # РЕЄСТРАЦІЯ
        with t2:
            with st.form("reg"):
                ne = st.text_input("Емейл")
                np = st.text_input("Пароль", type="password")
                c1, c2 = st.columns(2)
                fn = c1.text_input("Ім'я")
                ln = c2.text_input("Прізвище")
                if st.form_submit_button("Зареєструватися", use_container_width=True):
                    if ne and np and fn:
                        register_user(ne, np, fn, ln)
                    else:
                        st.warning("Всі поля обов'язкові.")


# =========================
# 5. ONBOARDING
# =========================


def onboarding_wizard():
    st.markdown("## 🚀 Налаштування Проекту")

    with st.container(border=True):
        step = st.session_state.get("onboarding_step", 2)

        if step == 2:
            st.subheader("Крок 1: Введіть дані про ваш бренд")

            brand = st.text_input("Назва бренду")
            domain = st.text_input("Домен (офіційний сайт)")
            industry = st.text_input("Галузь бренду / ніша")
            products = st.text_area(
                "Продукти / Послуги (перелічіть через кому або у стовпчик)"
            )

        if st.button("Згенерувати запити"):
            if brand and domain and industry and products:
                st.session_state["temp_brand"] = brand
                st.session_state["temp_domain"] = domain
                st.session_state["temp_industry"] = industry
                st.session_state["temp_products"] = products

                with st.spinner("Генеруємо релевантні запити через n8n AI Agent..."):
                    prompts = n8n_generate_prompts(brand, domain, industry, products)
                    if prompts and len(prompts) > 0:
                        st.session_state["generated_prompts"] = prompts
                        st.session_state["onboarding_step"] = 3
                        st.rerun()
                    else:
                        st.error("AI не повернув результатів. Спробуйте ще раз.")
            else:
                st.warning("Будь ласка, заповніть всі 4 поля.")

        elif step == 3:
            st.subheader("Крок 2: Оберіть 5 пріоритетних запитів")
            st.write(
                f"Оберіть 5 пріоритетних запитів для **{st.session_state['temp_brand']}**:"
            )

            opts = st.session_state.get("generated_prompts", [])
            selected = st.multiselect(
                "Список запитів:",
                opts,
                default=opts[:5] if len(opts) >= 5 else opts,
            )
            st.caption(f"Обрано: {len(selected)} / 5")

            if st.button("Запустити аналіз"):
                if len(selected) == 5:
                    with st.spinner("Створюємо проект та запускаємо аналіз..."):
                        try:
                            user_id = st.session_state["user"].id

                            res = (
                                supabase.table("projects")
                                .insert(
                                    {
                                        "user_id": user_id,
                                        "brand_name": st.session_state["temp_brand"],
                                        "domain": st.session_state["temp_domain"],
                                        "industry": st.session_state["temp_industry"],
                                        "products": st.session_state["temp_products"],
                                        "status": "trial",
                                    }
                                )
                                .execute()
                            )

                            if not res.data:
                                raise Exception("Project creation failed")

                            proj_data = res.data[0]
                            proj_id = proj_data["id"]

                            for kw in selected:
                                supabase.table("keywords").insert(
                                    {"project_id": proj_id, "keyword_text": kw}
                                ).execute()

                            n8n_trigger_analysis(
                                proj_id, selected, st.session_state["temp_brand"]
                            )

                            st.session_state["current_project"] = proj_data
                            st.success(
                                "Проект створено! Аналіз запущено у фоновому режимі."
                            )
                            time.sleep(2)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Системна помилка: {e}")
                else:
                    st.error("Будь ласка, оберіть рівно 5 запитів")


# =========================
# 6. DATA ACCESS HELPERS (ANALYTICS)
# =========================


def fetch_dashboard_stats(project_id: int):
    """Повертає агреговані метрики для дашборду."""
    sov = off = pos = pres = dom = coverage = 0
    freshness_days = None

    try:
        stats = (
            supabase.table("dashboard_stats")
            .select("*")
            .eq("project_id", project_id)
            .execute()
            .data
        )
        if stats:
            s = stats[0]
            sov = s.get("sov", 0)
            off = s.get("official_source_pct", 0)
            pos = s.get("avg_position", 0)
            pres = s.get("brand_presence_pct", 0)
            dom = s.get("domain_mentions_pct", 0)
            coverage = s.get("coverage_pct", 0)
            freshness_days = s.get("freshness_days")
    except Exception:
        pass

    return sov, off, pos, pres, dom, coverage, freshness_days


def fetch_keywords(project_id: int):
    """Отримує перелік ключових запитів."""
    try:
        kws = (
            supabase.table("keywords")
            .select("id, keyword_text, type, last_run_at, last_status")
            .eq("project_id", project_id)
            .order("id")
            .execute()
            .data
        )
        return kws or []
    except Exception:
        return []


def fetch_keyword_detail(project_id: int, keyword_text: str):
    """
    Повертає детальний аналіз по запиту:
    - expected response
    - aggregate status
    - responses by LLM
    """
    analysis = None
    responses = []

    try:
        res = (
            supabase.table("keyword_analysis")
            .select("*")
            .eq("project_id", project_id)
            .eq("keyword", keyword_text)
            .execute()
        )
        if res.data:
            analysis = res.data[0]
    except Exception:
        pass

    try:
        res2 = (
            supabase.table("llm_responses")
            .select("*")
            .eq("project_id", project_id)
            .eq("keyword", keyword_text)
            .order("llm_name")
            .execute()
        )
        responses = res2.data or []
    except Exception:
        responses = []

    return analysis, responses


def fetch_competitor_stats(project_id: int):
    """Статистика конкурентів для графіків."""
    try:
        res = (
            supabase.table("competitor_stats")
            .select("*")
            .eq("project_id", project_id)
            .execute()
        )
        return res.data or []
    except Exception:
        return []


def fetch_source_stats(project_id: int):
    """Розподіл джерел для нашого бренду і конкурентів."""
    try:
        res = (
            supabase.table("source_stats")
            .select("*")
            .eq("project_id", project_id)
            .execute()
        )
        return res.data or []
    except Exception:
        return []


# =========================
# 7. DASHBOARD
# =========================


def show_dashboard():
    proj = st.session_state.get("current_project", {})
    if not proj:
        st.info("Проект не знайдено.")
        return

    project_id = proj["id"]

    # Header
    c1, c2 = st.columns([3, 1])
    with c1:
        st.title(f"Дашборд: {proj.get('brand_name', 'Brand')}")
        st.caption(f"Домен: {proj.get('domain', '—')}")
    with c2:
        st.selectbox("Період:", ["Останні 7 днів", "Останні 30 днів"], index=0)
    st.markdown("---")

    # KPI
    sov, off, pos, pres, dom, coverage, freshness_days = fetch_dashboard_stats(
        project_id
    )

    k1, k2, k3 = st.columns(3)
    with k1:
        with st.container(border=True):
            st.markdown("**ЧАСТКА ГОЛОСУ (SOV)**", help=METRIC_TOOLTIPS["sov"])
            c, ch = st.columns([1, 1])
            c.markdown(f"## {sov}%")
            ch.plotly_chart(
                get_donut_chart(sov), use_container_width=True, key="kpi_sov"
            )

    with k2:
        with st.container(border=True):
            st.markdown("**% ОФІЦІЙНИХ ДЖЕРЕЛ**", help=METRIC_TOOLTIPS["official"])
            c, ch = st.columns([1, 1])
            c.markdown(f"## {off}%")
            ch.plotly_chart(
                get_donut_chart(off), use_container_width=True, key="kpi_off"
            )

    with k3:
        with st.container(border=True):
            st.markdown("**ПОКРИТТЯ ЗАПИТІВ**", help=METRIC_TOOLTIPS["coverage"])
            c, ch = st.columns([1, 1])
            c.markdown(f"## {coverage}%")
            ch.plotly_chart(
                get_donut_chart(coverage), use_container_width=True, key="kpi_cov"
            )

    k4, k5, k6 = st.columns(3)
    with k4:
        with st.container(border=True):
            st.markdown("**ПОЗИЦІЯ БРЕНДУ**", help=METRIC_TOOLTIPS["position"])
            st.markdown(
                f"<h1 style='text-align: center; color: #8041F6;'>{pos}</h1>",
                unsafe_allow_html=True,
            )
            st.progress(int(100 - (pos * 10)) if pos else 0)

    with k5:
        with st.container(border=True):
            st.markdown("**ПРИСУТНІСТЬ БРЕНДУ**", help=METRIC_TOOLTIPS["presence"])
            c, ch = st.columns([1, 1])
            c.markdown(f"## {pres}%")
            ch.plotly_chart(
                get_donut_chart(pres), use_container_width=True, key="kpi_pres"
            )

    with k6:
        with st.container(border=True):
            st.markdown("**ЗГАДКИ ДОМЕНУ**", help=METRIC_TOOLTIPS["domain"])
            c, ch = st.columns([1, 1])
            c.markdown(f"## {dom}%")
            ch.plotly_chart(
                get_donut_chart(dom), use_container_width=True, key="kpi_dom"
            )

    # Freshness
    if freshness_days is not None:
        st.info(f"Середній вік відповідей LLM: {freshness_days} днів.")

    st.markdown("### 📈 Тренди SOV / Sentiment / Присутності")

    trend_cols = st.columns(2)
    with trend_cols[0]:
        try:
            ts = (
                supabase.table("dashboard_stats_history")
                .select("date, sov, brand_presence_pct")
                .eq("project_id", project_id)
                .order("date")
                .execute()
                .data
            )
        except Exception:
            ts = []

        if ts:
            df = pd.DataFrame(ts)
            fig = go.Figure()
            fig.add_trace(
                go.Scatter(
                    x=df["date"], y=df["sov"], mode="lines+markers", name="SOV"
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=df["date"],
                    y=df["brand_presence_pct"],
                    mode="lines+markers",
                    name="Присутність",
                )
            )
            fig.update_layout(
                height=280,
                margin=dict(l=0, r=0, t=30, b=0),
                legend=dict(orientation="h"),
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Трендові дані ще не зібрані.")

    with trend_cols[1]:
        try:
            ts2 = (
                supabase.table("sentiment_history")
                .select("date, positive_pct, neutral_pct, negative_pct")
                .eq("project_id", project_id)
                .order("date")
                .execute()
                .data
            )
        except Exception:
            ts2 = []

        if ts2:
            df2 = pd.DataFrame(ts2)
            fig2 = go.Figure()
            fig2.add_trace(
                go.Scatter(
                    x=df2["date"],
                    y=df2["positive_pct"],
                    mode="lines",
                    name="Positive",
                )
            )
            fig2.add_trace(
                go.Scatter(
                    x=df2["date"],
                    y=df2["neutral_pct"],
                    mode="lines",
                    name="Neutral",
                )
            )
            fig2.add_trace(
                go.Scatter(
                    x=df2["date"],
                    y=df2["negative_pct"],
                    mode="lines",
                    name="Negative",
                )
            )
            fig2.update_layout(
                height=280,
                margin=dict(l=0, r=0, t=30, b=0),
                legend=dict(orientation="h"),
            )
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("Динаміка тональності ще не доступна.")

    st.markdown("### 🆚 Порівняння з конкурентами")

    comp_stats = fetch_competitor_stats(project_id)
    if comp_stats:
        dfc = pd.DataFrame(comp_stats)
        cols = st.columns(2)

        with cols[0]:
            # SOV by brand vs competitors
            fig = go.Figure()
            fig.add_trace(
                go.Bar(
                    x=dfc["brand_name"],
                    y=dfc["sov"],
                    name="SOV",
                )
            )
            fig.update_layout(
                height=280,
                margin=dict(l=0, r=0, t=30, b=0),
                xaxis_title="Бренд",
                yaxis_title="SOV %",
            )
            st.plotly_chart(fig, use_container_width=True)

        with cols[1]:
            # Official sources share
            if "official_source_pct" in dfc.columns:
                fig3 = go.Figure()
                fig3.add_trace(
                    go.Bar(
                        x=dfc["brand_name"],
                        y=dfc["official_source_pct"],
                        name="% офіційних джерел",
                    )
                )
                fig3.update_layout(
                    height=280,
                    margin=dict(l=0, r=0, t=30, b=0),
                    xaxis_title="Бренд",
                    yaxis_title="% офіційних джерел",
                )
                st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("Дані по конкурентам ще не готові.")

    st.markdown("### 🌐 Джерела відповідей моделей")

    src_stats = fetch_source_stats(project_id)
    if src_stats:
        dfs = pd.DataFrame(src_stats)
        cols2 = st.columns(2)

        with cols2[0]:
            # Розподіл типів джерел для нашого бренду
            df_brand = dfs[dfs["brand_name"] == proj.get("brand_name")]
            if not df_brand.empty:
                fig4 = go.Figure(
                    data=[
                        go.Pie(
                            labels=df_brand["source_type"],
                            values=df_brand["share_pct"],
                            hole=0.4,
                        )
                    ]
                )
                fig4.update_layout(
                    height=260,
                    margin=dict(l=0, r=0, t=30, b=0),
                    showlegend=True,
                )
                st.plotly_chart(fig4, use_container_width=True)
            else:
                st.info("Немає джерел для основного бренду.")

        with cols2[1]:
            # Порівняння кількості офіційних джерел між брендами
            if "official_sources_count" in dfs.columns:
                fig5 = go.Figure(
                    data=[
                        go.Bar(
                            x=dfs["brand_name"],
                            y=dfs["official_sources_count"],
                            name="К-сть офіційних джерел",
                        )
                    ]
                )
                fig5.update_layout(
                    height=260,
                    margin=dict(l=0, r=0, t=30, b=0),
                    xaxis_title="Бренд",
                )
                st.plotly_chart(fig5, use_container_width=True)
    else:
        st.info("Дані про джерела поки відсутні.")

    st.markdown("### 📋 Усі запити (швидкий перехід до деталей)")

    keywords = fetch_keywords(project_id)
    if not keywords:
        st.info("Запити ще не додані.")
        return

    for kw in keywords:
        cols = st.columns([6, 1])
        with cols[0]:
            st.markdown(f"- **{kw['keyword_text']}**")
        with cols[1]:
            if st.button("Деталі", key=f"go_{kw['id']}"):
                st.session_state["requested_page"] = "Перелік запитів"
                st.session_state["focus_keyword"] = kw["keyword_text"]
                st.rerun()


# =========================
# 8. QUERIES PAGE (CRUD + MANUAL ANALYSIS)
# =========================


def show_queries_page():
    proj = st.session_state.get("current_project", {})
    if not proj:
        st.info("Проект не знайдено.")
        return

    project_id = proj["id"]
    st.title("📋 Перелік запитів")

    # Додавання нового запиту
    with st.expander("➕ Додати новий запит"):
        with st.form("add_kw_form"):
            new_kw = st.text_input("Новий запит")
            kw_type = st.selectbox(
                "Тип запиту",
                ["ranking", "accuracy", "comparative", "event"],
                index=0,
            )
            if st.form_submit_button("Додати"):
                if new_kw:
                    try:
                        supabase.table("keywords").insert(
                            {
                                "project_id": project_id,
                                "keyword_text": new_kw,
                                "type": kw_type,
                            }
                        ).execute()
                        st.success("Запит додано.")
                        st.experimental_rerun()
                    except Exception as e:
                        st.error(f"Помилка додавання: {e}")
                else:
                    st.warning("Введіть текст запиту.")

    keywords = fetch_keywords(project_id)
    if not keywords:
        st.info("Запити поки що відсутні.")
        return

    st.markdown("### ✏️ Редагування та запуск аналізу")

    selected_for_analysis = []

    for kw in keywords:
        kw_id = kw["id"]
        col1, col2, col3, col4 = st.columns([6, 1.5, 1.5, 1.5])
        with col1:
            new_text = st.text_input(
                "Запит",
                value=kw["keyword_text"],
                key=f"kw_txt_{kw_id}",
                label_visibility="collapsed",
            )
        with col2:
            if st.button("💾 Зберегти", key=f"save_{kw_id}"):
                try:
                    supabase.table("keywords").update(
                        {"keyword_text": new_text}
                    ).eq("id", kw_id).execute()
                    st.success("Збережено.")
                except Exception as e:
                    st.error(f"Помилка збереження: {e}")
        with col3:
            if st.button("🗑️ Видалити", key=f"del_{kw_id}"):
                try:
                    supabase.table("keywords").delete().eq("id", kw_id).execute()
                    st.success("Видалено.")
                    st.experimental_rerun()
                except Exception as e:
                    st.error(f"Помилка видалення: {e}")
        with col4:
            if st.checkbox("Для аналізу", key=f"chk_{kw_id}"):
                selected_for_analysis.append(new_text)

    st.markdown("---")
    st.markdown("### ⚙️ Запуск аналізу вибраних запитів")

    llm_choices = ["ChatGPT", "Claude", "Gemini", "Perplexity"]
    llms_selected = st.multiselect(
        "Оберіть LLM, які мають аналізувати запити", llm_choices, default=["ChatGPT"]
    )

    if st.button("🚀 Надіслати вибрані запити в n8n"):
        if not selected_for_analysis:
            st.warning("Спочатку оберіть хоча б один запит.")
        else:
            with st.spinner("Запускаємо аналіз у n8n..."):
                ok = n8n_trigger_analysis(
                    project_id,
                    selected_for_analysis,
                    proj.get("brand_name", ""),
                    llms=llms_selected,
                )
                if ok:
                    st.success("Аналіз запущено.")
                else:
                    st.error("Не вдалося запустити аналіз.")

    st.markdown("---")
    st.markdown("### 🔍 Детальна картка запиту")

    # Обираємо фокусний запит
    all_kw_texts = [k["keyword_text"] for k in keywords]
    default_index = 0
    if (
        st.session_state.get("focus_keyword")
        and st.session_state["focus_keyword"] in all_kw_texts
    ):
        default_index = all_kw_texts.index(st.session_state["focus_keyword"])

    selected_kw = st.selectbox(
        "Оберіть запит для деталізації",
        all_kw_texts,
        index=default_index,
    )

    analysis, responses = fetch_keyword_detail(project_id, selected_kw)

    st.markdown(f"#### 🔗 {selected_kw}")

    with st.container():
        st.markdown('<div class="query-detail-box">', unsafe_allow_html=True)

        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("##### Expected Response")
            if analysis and analysis.get("expected_response"):
                st.info(analysis["expected_response"])
            else:
                st.caption("Очікувана відповідь ще не налаштована.")

        with col_b:
            st.markdown("##### Current Status")
            if analysis:
                st.write(f"**Точність:** {analysis.get('accuracy_label', '—')}")
                st.write(f"**Позиція:** {analysis.get('rank_position', '—')}")
                st.write(f"**Останній запуск:** {analysis.get('last_run_at', '—')}")
            else:
                st.caption("Дані аналізу ще не доступні.")

        st.markdown("##### Provider Responses")

        if responses:
            for r in responses:
                st.markdown(f"**{r.get('llm_name', 'LLM')}**")

                status = r.get("status_label", "unknown")
                if status.lower() == "correct":
                    st.success("Correct")
                elif status.lower() == "partial":
                    st.warning("Partially correct")
                else:
                    st.error("Incorrect")

                st.markdown("**Відповідь:**")
                st.write(r.get("answer_text", ""))

                if r.get("justification"):
                    st.markdown("**Justification:**")
                    st.caption(r["justification"])

                if r.get("sources"):
                    st.markdown("**Sources:**")
                    for s in r["sources"]:
                        st.markdown(f"- [{s}]({s})")
                st.markdown("---")
        else:
            st.caption(
                "Відповіді LLM ще не збережені. Запустіть аналіз або дочекайтесь його завершення."
            )

        st.markdown("</div>", unsafe_allow_html=True)


# =========================
# 9. AI SERP EXPLORER (BASIC)
# =========================


def show_ai_serp_explorer():
    proj = st.session_state.get("current_project", {})
    if not proj:
        st.info("Проект не знайдено.")
        return

    project_id = proj["id"]
    st.title("🔎 AI SERP Explorer")

    llm_filter = st.multiselect(
        "Оберіть LLM для перегляду",
        ["ChatGPT", "Claude", "Gemini", "Perplexity"],
        default=["ChatGPT"],
    )

    try:
        res = (
            supabase.table("llm_responses")
            .select("keyword, llm_name, status_label, rank_position, last_run_at")
            .eq("project_id", project_id)
            .in_("llm_name", llm_filter)
            .execute()
        )
        data = res.data or []
    except Exception:
        data = []

    if not data:
        st.info("Дані AI SERP ще не зібрані.")
        return

    df = pd.DataFrame(data)
    st.dataframe(df, use_container_width=True, hide_index=True)


# =========================
# 10. SIDEBAR
# =========================


def sidebar_menu():
    with st.sidebar:
        st.markdown(
            '<div class="sidebar-logo-container"><img src="https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png"></div>',
            unsafe_allow_html=True,
        )

        if st.session_state["role"] == "admin":
            st.markdown("### 🛠 Admin Select")
            try:
                projs = supabase.table("projects").select("*").execute().data
                if projs:
                    opts = {p["brand_name"]: p for p in projs}
                    sel = st.selectbox("Project", list(opts.keys()))
                    if (
                        st.session_state.get("current_project", {}).get("brand_name")
                        != sel
                    ):
                        st.session_state["current_project"] = opts[sel]
                        st.rerun()
            except Exception:
                pass

        st.divider()

        if st.session_state.get("current_project"):
            p = st.session_state["current_project"]
            st.markdown(
                "<div class='sidebar-label'>Current Brand</div>",
                unsafe_allow_html=True,
            )
            badge = (
                "<span class='badge-trial'>TRIAL</span>"
                if p.get("status") == "trial"
                else "<span class='badge-active'>PRO</span>"
            )
            st.markdown(
                f"**{p.get('brand_name') or p.get('name')}** {badge}",
                unsafe_allow_html=True,
            )

            if p.get("status") == "trial":
                st.markdown(
                    '<a href="mailto:hi@virshi.ai" class="upgrade-btn">⭐ Підвищити план</a>',
                    unsafe_allow_html=True,
                )
            st.divider()

        opts = [
            "Дашборд",
            "Перелік запитів",
            "Джерела",
            "Конкуренти",
            "Рекомендації",
            "AI SERP Explorer",
            "GPT-Visibility",
        ]
        icons = [
            "speedometer2",
            "list-ul",
            "hdd-network",
            "people",
            "lightbulb",
            "search",
            "robot",
        ]

        if st.session_state["role"] == "admin":
            opts.append("Адмін")
            icons.append("shield-lock")

        default_index = 0
        if st.session_state.get("requested_page") and st.session_state[
            "requested_page"
        ] in opts:
            default_index = opts.index(st.session_state["requested_page"])
            st.session_state["requested_page"] = None

        selected = option_menu(
            menu_title=None,
            options=opts,
            icons=icons,
            menu_icon="cast",
            default_index=default_index,
            styles={
                "nav-link-selected": {"background-color": "#8041F6"},
                "container": {"padding": "0!important"},
            },
        )
        st.divider()

        if st.session_state["user"]:
            d = st.session_state.get("user_details", {})
            full = f"{d.get('first_name','')} {d.get('last_name','')}".strip()
            st.markdown(
                f"<div class='sidebar-name'>{full}</div>", unsafe_allow_html=True
            )
            st.markdown("**Support:** [hi@virshi.ai](mailto:hi@virshi.ai)")
            if st.button("Вийти"):
                logout()

    return selected


# =========================
# 11. ROUTER
# =========================


def main():
    check_session()

    if not st.session_state["user"]:
        login_page()

    elif (
        st.session_state.get("current_project") is None
        and st.session_state["role"] != "admin"
    ):
        with st.sidebar:
            if st.button("Вийти"):
                logout()
        onboarding_wizard()

    else:
        if st.session_state["role"] == "admin" and not st.session_state.get(
            "current_project"
        ):
            pass

        page = sidebar_menu()

        if page == "Дашборд":
            show_dashboard()
        elif page == "Перелік запитів":
            show_queries_page()
        elif page == "Джерела":
            st.title("📡 Джерела")
            st.info("Детальні source-графіки вже на головному дашборді.")
        elif page == "Конкуренти":
            st.title("⚔️ Конкуренти")
            st.info("Аналітика конкурентів також показана на дашборді.")
        elif page == "Рекомендації":
            st.title("💡 Рекомендації")
            st.info("У розробці...")
        elif page == "AI SERP Explorer":
            show_ai_serp_explorer()
        elif page == "GPT-Visibility":
            st.title("🤖 GPT-Visibility")
            st.info("У розробці...")
        elif page == "Адмін":
            st.title("🛡️ Admin Panel")
            try:
                df = pd.DataFrame(
                    supabase.table("projects").select("*").execute().data
                )
                st.dataframe(df, use_container_width=True)
            except Exception:
                st.error("Помилка доступу до БД.")


if __name__ == "__main__":
    main()
