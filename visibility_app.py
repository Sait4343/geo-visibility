import time
from datetime import datetime, timedelta, date

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
N8N_RECO_URL = "https://virshi.app.n8n.cloud/webhook/recommendations"  # за потреби заміниш

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
    st.session_state["onboarding_step"] = 2  # стартуємо одразу з кроку про бренд
if "focus_keyword_id" not in st.session_state:
    st.session_state["focus_keyword_id"] = None

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
}


def n8n_generate_prompts(brand: str, domain: str, industry: str, products: str):
    """
    Викликає n8n вебхук для генерації промптів.
    Відправляємо всі 4 параметри: бренд, домен, галузь, продукти/послуги.
    """
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


def n8n_trigger_analysis(project_id, keywords, brand_name, models=None):
    """
    Відправляє вибрані запити на n8n для глибокого аналізу.
    n8n сам пише результати в Supabase.
    """
    try:
        user_email = st.session_state["user"].email if st.session_state.get("user") else None
        payload = {
            "project_id": project_id,
            "keywords": keywords,
            "brand_name": brand_name,
            "user_email": user_email,
            "models": models or [],
        }
        requests.post(N8N_ANALYZE_URL, json=payload, timeout=2)
        return True
    except requests.exceptions.ReadTimeout:
        return True
    except Exception as e:
        st.error(f"Помилка запуску аналізу: {e}")
        return False


def n8n_request_recommendations(project, topic: str, brief: str):
    """
    Надсилає запит на n8n для генерації рекомендацій.
    topic: 'pr' | 'digital' | 'creative'
    """
    try:
        payload = {
            "project_id": project["id"],
            "brand_name": project.get("brand_name"),
            "domain": project.get("domain"),
            "topic": topic,
            "brief": brief,
            "user_email": st.session_state["user"].email
            if st.session_state.get("user")
            else None,
        }
        resp = requests.post(N8N_RECO_URL, json=payload, timeout=40)
        if resp.status_code != 200:
            st.error(f"N8N recommendation error: {resp.status_code} - {resp.text}")
            return []

        data = resp.json()
        if isinstance(data, list):
            return data
        return data.get("recommendations", [])
    except Exception as e:
        st.error(f"Помилка запиту рекомендацій: {e}")
        return []


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
                if getattr(res, "user", None):
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
    """
    Реєстрація нового користувача + запис first_name / last_name в таблицю profiles.
    """
    try:
        res = supabase.auth.sign_up(
            {
                "email": email,
                "password": password,
                "options": {"data": {"first_name": first, "last_name": last}},
            }
        )

        if res.user:
            # явне створення профілю
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
    st.session_state["focus_keyword_id"] = None
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

        # STEP 2 – дані про бренд
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

        # STEP 3 – вибір 5 запитів
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

                            # 1. Створюємо проект
                            res = (
                                supabase.table("projects")
                                .insert(
                                    {
                                        "user_id": user_id,
                                        "brand_name": st.session_state["temp_brand"],
                                        "domain": st.session_state["temp_domain"],
                                        "industry": st.session_state[
                                            "temp_industry"
                                        ],
                                        "products": st.session_state[
                                            "temp_products"
                                        ],
                                        "status": "trial",
                                    }
                                )
                                .execute()
                            )

                            if not res.data:
                                raise Exception("Project creation failed")

                            proj_data = res.data[0]
                            proj_id = proj_data["id"]

                            # 2. Записуємо ключові слова
                            for kw in selected:
                                supabase.table("keywords").insert(
                                    {
                                        "project_id": proj_id,
                                        "keyword_text": kw,
                                        "type": "ranking",
                                    }
                                ).execute()

                            # 3. Запускаємо аналіз через n8n
                            n8n_trigger_analysis(
                                proj_id, selected, st.session_state["temp_brand"]
                            )

                            # 4. Фінал
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
# 6. DASHBOARD
# =========================


def show_dashboard():
    proj = st.session_state.get("current_project", {})

    c1, c2 = st.columns([3, 1])
    with c1:
        st.title(f"Дашборд: {proj.get('brand_name', 'Brand')}")
    with c2:
        st.selectbox("Період:", ["Останні 7 днів", "Останні 30 днів"], index=0)
    st.markdown("---")

    sov, off, pos, pres, dom = 0, 0, 0, 0, 0
    try:
        stats = (
            supabase.table("dashboard_stats")
            .select("*")
            .eq("project_id", proj["id"])
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
    except Exception:
        pass

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
            st.markdown(
                "**% ОФІЦІЙНИХ ДЖЕРЕЛ**", help=METRIC_TOOLTIPS["official"]
            )
            c, ch = st.columns([1, 1])
            c.markdown(f"## {off}%")
            ch.plotly_chart(
                get_donut_chart(off), use_container_width=True, key="kpi_off"
            )
    with k3:
        with st.container(border=True):
            st.markdown(
                "**ЗАГАЛЬНИЙ НАСТРІЙ**", help=METRIC_TOOLTIPS["sentiment"]
            )
            fig = go.Figure(
                data=[
                    go.Pie(
                        labels=["Pos", "Neu", "Neg"],
                        values=[60, 30, 10],
                        hole=0,
                        marker_colors=["#00C896", "#9EA0A5", "#FF4B4B"],
                    )
                ]
            )
            fig.update_layout(
                height=80,
                margin=dict(t=0, b=0, l=0, r=0),
                showlegend=False,
            )
            st.plotly_chart(fig, use_container_width=True, key="kpi_sent")

    k4, k5, k6 = st.columns(3)
    with k4:
        with st.container(border=True):
            st.markdown(
                "**ПОЗИЦІЯ БРЕНДУ**", help=METRIC_TOOLTIPS["position"]
            )
            st.markdown(
                f"<h1 style='text-align: center; color: #8041F6;'>{pos}</h1>",
                unsafe_allow_html=True,
            )
            st.progress(int(100 - (pos * 10)) if pos else 0)
    with k5:
        with st.container(border=True):
            st.markdown(
                "**ПРИСУТНІСТЬ БРЕНДУ**", help=METRIC_TOOLTIPS["presence"]
            )
            c, ch = st.columns([1, 1])
            c.markdown(f"## {pres}%")
            ch.plotly_chart(
                get_donut_chart(pres), use_container_width=True, key="kpi_pres"
            )
    with k6:
        with st.container(border=True):
            st.markdown(
                "**ЗГАДКИ ДОМЕНУ**", help=METRIC_TOOLTIPS["domain"]
            )
            c, ch = st.columns([1, 1])
            c.markdown(f"## {dom}%")
            ch.plotly_chart(
                get_donut_chart(dom), use_container_width=True, key="kpi_dom"
            )

    st.markdown("### 📋 Моніторинг запитів")
    try:
        kws = (
            supabase.table("keywords")
            .select("id, keyword_text, type")
            .eq("project_id", proj["id"])
            .execute()
            .data
        )
    except Exception:
        kws = []

    if not kws:
        st.info("Дані ще збираються. Оновіть сторінку за хвилину.")
        return

    # короткий список + кнопка переходу до детального екрану
    for k in kws:
        col1, col2 = st.columns([4, 1])
        with col1:
            st.write(f"- {k.get('keyword_text')}")
        with col2:
            if st.button("➡ Детально", key=f"goto_kw_{k['id']}"):
                st.session_state["focus_keyword_id"] = k["id"]
                # переключаємо сторінку на "Перелік запитів"
                st.session_state["force_page"] = "Перелік запитів"
                st.rerun()


# =========================
# 7. КЕРУВАННЯ ЗАПИТАМИ
# =========================


def show_keywords_page():
    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект в онбордингу.")
        return

    st.title("📋 Перелік запитів")

    # --- Форма додавання нового запиту ---
    with st.form("add_keyword_form"):
        new_kw = st.text_input("Новий запит")
        new_type = st.selectbox(
            "Тип запиту", ["ranking", "accuracy", "other"], index=0
        )
        add_submitted = st.form_submit_button("Додати")
        if add_submitted:
            if not new_kw:
                st.warning("Введіть текст запиту.")
            else:
                try:
                    supabase.table("keywords").insert(
                        {
                            "project_id": proj["id"],
                            "keyword_text": new_kw,
                            "type": new_type,
                        }
                    ).execute()
                    st.success("Запит додано.")
                    st.rerun()
                except Exception as e:
                    st.error(
                        f"Помилка додавання: {getattr(e, 'args', [str(e)])[0]}"
                    )

    st.markdown("---")
    st.markdown("### Поточні запити")

    try:
        resp = (
            supabase.table("keywords")
            .select("*")
            .eq("project_id", proj["id"])
            .order("id")
            .execute()
        )
        keywords = resp.data or []
    except Exception as e:
        st.error(f"Помилка завантаження: {e}")
        keywords = []

    if not keywords:
        st.info("Запити поки що відсутні.")
        return

    # --- Вибір запитів для аналізу в n8n ---
    st.markdown("#### Виберіть запити для аналізу в n8n")

    kw_labels = [k["keyword_text"] for k in keywords]
    selected_labels = st.multiselect(
        "Запити для аналізу:", kw_labels, key="kw_for_n8n"
    )

    model_choices = ["chatgpt", "claude", "gemini"]
    selected_models = st.multiselect(
        "Які LLM використовувати:",
        model_choices,
        default=["chatgpt", "gemini"],
    )

    if st.button("🔍 Запустити аналіз у n8n"):
        if not selected_labels:
            st.warning("Оберіть щонайменше один запит.")
        else:
            try:
                n8n_trigger_analysis(
                    proj["id"],
                    selected_labels,
                    proj.get("brand_name"),
                    models=selected_models,
                )
                st.success("Аналіз запущено в n8n.")
            except Exception as e:
                st.error(f"Не вдалося відправити запити в n8n: {e}")

    st.markdown("#### Редагування запитів")

    for k in keywords:
        expanded = (
            st.session_state.get("focus_keyword_id") == k["id"]
            if st.session_state.get("focus_keyword_id")
            else False
        )
        with st.expander(
            k.get("keyword_text", "") or "Запит", expanded=expanded
        ):
            col1, col2 = st.columns([3, 1])
            with col1:
                txt = st.text_input(
                    "Текст запиту",
                    value=k.get("keyword_text", ""),
                    key=f"kw_txt_{k['id']}",
                )
            with col2:
                ktype = st.selectbox(
                    "Тип",
                    ["ranking", "accuracy", "other"],
                    index=(
                        ["ranking", "accuracy", "other"].index(k.get("type", "ranking"))
                        if k.get("type") in ["ranking", "accuracy", "other"]
                        else 0
                    ),
                    key=f"kw_type_{k['id']}",
                )

            c_save, c_delete = st.columns(2)
            if c_save.button("💾 Зберегти", key=f"save_kw_{k['id']}"):
                try:
                    supabase.table("keywords").update(
                        {"keyword_text": txt, "type": ktype}
                    ).eq("id", k["id"]).execute()
                    st.success("Збережено.")
                    st.session_state["focus_keyword_id"] = k["id"]
                    st.rerun()
                except Exception as e:
                    st.error(f"Помилка збереження: {e}")

            if c_delete.button("🗑 Видалити", key=f"del_kw_{k['id']}"):
                try:
                    supabase.table("keywords").delete().eq("id", k["id"]).execute()
                    st.success("Видалено.")
                    if st.session_state.get("focus_keyword_id") == k["id"]:
                        st.session_state["focus_keyword_id"] = None
                    st.rerun()
                except Exception as e:
                    st.error(f"Помилка видалення: {e}")

    # після першого відкриття скидаємо фокус, щоб не застрягати
    st.session_state["focus_keyword_id"] = None


# =========================
# 8. РЕКОМЕНДАЦІЇ
# =========================


def show_recommendations_page():
    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект, щоб отримувати рекомендації.")
        return

    st.title("💡 Рекомендації")

    tabs = st.tabs(["PR", "Digital", "Creative"])

    topics = ["pr", "digital", "creative"]
    labels = ["PR / Комунікації", "Digital / Performance", "Creative / Ідеї"]

    for tab, topic, label in zip(tabs, topics, labels):
        with tab:
            st.markdown(f"### {label}")

            with st.form(f"reco_form_{topic}"):
                brief = st.text_area(
                    "Коротко опишіть задачу / контекст (укр / англ)",
                    height=120,
                )
                submitted = st.form_submit_button("Запросити рекомендації")
                if submitted:
                    if not brief.strip():
                        st.warning("Будь ласка, опишіть задачу.")
                    else:
                        with st.spinner("Генеруємо рекомендації через n8n..."):
                            recos = n8n_request_recommendations(proj, topic, brief)
                            if recos:
                                st.success("Рекомендації отримано.")
                                # опційно — зберігаємо в БД
                                try:
                                    rows = [
                                        {
                                            "project_id": proj["id"],
                                            "topic": topic,
                                            "created_at": datetime.utcnow().isoformat(),
                                            "title": r.get("title", "")[:255],
                                            "summary": r.get("summary", ""),
                                            "details": r.get("details", ""),
                                        }
                                        for r in recos
                                    ]
                                    supabase.table("recommendations").insert(
                                        rows
                                    ).execute()
                                except Exception:
                                    # якщо таблиці ще немає — просто ігноруємо
                                    pass

    st.markdown("---")
    st.markdown("### Історія рекомендацій")

    # фільтр по даті
    c1, c2 = st.columns(2)
    with c1:
        date_from = st.date_input(
            "З дати",
            value=date.today().replace(day=1),
        )
    with c2:
        date_to = st.date_input("По дату", value=date.today())

    try:
        q = (
            supabase.table("recommendations")
            .select("*")
            .eq("project_id", proj["id"])
            .order("created_at", desc=True)
        )
        data = q.execute().data or []
    except Exception:
        data = []

    if not data:
        st.info("Поки що рекомендацій немає або таблиця recommendations не створена.")
        return

    # фільтруємо по даті
    filtered = []
    for r in data:
        try:
            dt = datetime.fromisoformat(str(r.get("created_at")).replace("Z", "+00:00"))
        except Exception:
            continue
        if date_from <= dt.date() <= date_to:
            filtered.append(r)

    if not filtered:
        st.info("Немає рекомендацій за обраний період.")
        return

    for r in filtered:
        dt = str(r.get("created_at", ""))[:19]
        topic = r.get("topic", "")
        title = r.get("title") or "(без назви)"
        header = f"[{dt}] {topic.upper()} — {title}"
        with st.expander(header):
            st.markdown(f"**Коротко:** {r.get('summary','')}")
            st.markdown("---")
            st.markdown(r.get("details", "") or "_Без деталей_")


# =========================
# 9. SIDEBAR
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
                        st.session_state.get("current_project", {}).get(
                            "brand_name"
                        )
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
        ]
        icons = ["speedometer2", "list-ul", "hdd-network", "people", "lightbulb"]

        opts.append("GPT-Visibility")
        icons.append("robot")

        if st.session_state["role"] == "admin":
            opts.append("Адмін")
            icons.append("shield-lock")

        default_index = 0
        if st.session_state.get("force_page") in opts:
            default_index = opts.index(st.session_state["force_page"])
            st.session_state["force_page"] = None

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
# 10. ROUTER
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
            show_keywords_page()
        elif page == "Джерела":
            st.title("📡 Джерела")
            st.info("У розробці...")
        elif page == "Конкуренти":
            st.title("⚔️ Конкуренти")
            st.info("У розробці...")
        elif page == "Рекомендації":
            show_recommendations_page()
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
