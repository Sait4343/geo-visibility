import time
from datetime import datetime, timedelta, date, time as dt_time # Додано dt_time
import plotly.express as px 
import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
import extra_streamlit_components as stx
from streamlit_option_menu import option_menu
from supabase import create_client, Client
import numpy as np # Потрібно для адмінки
import json


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
N8N_ANALYZE_URL = "https://virshi.app.n8n.cloud/webhook/webhook/run-analysis_prod"
N8N_RECO_URL = "https://virshi.app.n8n.cloud/webhook/recommendations"  # за потреби заміниш
N8N_CHAT_WEBHOOK = "https://virshi.app.n8n.cloud/webhook-test/webhook/chat-bot" 


# Custom CSS
st.markdown(
    """
<style>
    /* 1. ЗАГАЛЬНІ НАЛАШТУВАННЯ */
    .stApp { background-color: #F4F6F9; }
    
    /* Приховування якірних посилань (ланцюжків) біля заголовків */
    [data-testid="stMarkdownContainer"] h1 > a,
    [data-testid="stMarkdownContainer"] h2 > a,
    [data-testid="stMarkdownContainer"] h3 > a,
    [data-testid="stMarkdownContainer"] h4 > a,
    [data-testid="stMarkdownContainer"] h5 > a,
    [data-testid="stMarkdownContainer"] h6 > a {
        display: none !important;
    }
    a.anchor-link { display: none !important; }

    /* 2. САЙДБАР */
    section[data-testid="stSidebar"] { 
        background-color: #FFFFFF; 
        border-right: 1px solid #E0E0E0; 
    }
    .sidebar-logo-container { display: flex; justify-content: center; margin-bottom: 10px; }
    .sidebar-logo-container img { width: 140px; }
    .sidebar-name { font-size: 14px; font-weight: 600; color: #333; margin-top: 5px;}
    .sidebar-label { font-size: 11px; color: #999; text-transform: uppercase; letter-spacing: 0.5px; margin-top: 15px;}

    /* 3. КОНТЕЙНЕРИ І ФОРМИ */
    .css-1r6slb0, .css-12oz5g7, div[data-testid="stForm"] {
        background-color: white; padding: 20px; border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05); border: 1px solid #EAEAEA;
    }

    /* 4. МЕТРИКИ */
    div[data-testid="stMetric"] {
        background-color: #ffffff; border: 1px solid #e0e0e0; padding: 15px;
        border-radius: 10px; text-align: center; box-shadow: 0 1px 3px rgba(0,0,0,0.05);
    }
    .metric-card-small {
        background-color: #F0F2F6;
        border-radius: 6px;
        padding: 10px;
        text-align: center;
    }
    .metric-value {
        font-size: 18px; font-weight: bold; color: #8041F6;
    }
    .metric-label {
        font-size: 12px; color: #666;
    }

    /* 5. КНОПКИ */
    .stButton>button { 
        background-color: #8041F6; color: white; border-radius: 8px; border: none; font-weight: 600; 
        transition: background-color 0.3s;
    }
    .stButton>button:hover { background-color: #6a35cc; }
    
    .upgrade-btn {
        display: block; width: 100%; background-color: #FFC107; color: #000000;
        text-align: center; padding: 8px; border-radius: 8px;
        text-decoration: none; font-weight: bold; margin-top: 10px; border: 1px solid #e0a800;
    }

    /* 6. БЕЙДЖІ ТА СТАТУСИ */
    .badge-trial { background-color: #FFECB3; color: #856404; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.7em; }
    .badge-active { background-color: #D4EDDA; color: #155724; padding: 2px 6px; border-radius: 4px; font-weight: bold; font-size: 0.7em; }

    /* 7. ВІДПОВІДЬ ШІ */
    .ai-response-box {
        background-color: #ffffff;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 20px;
        font-family: 'Source Sans Pro', sans-serif;
        line-height: 1.6;
        color: #31333F;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05);
        max-height: 600px;
        overflow-y: auto;
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
    Відправляє запит на n8n для аналізу.
    Оновлено: Додано авторизацію (virshi-auth).
    """
    
    # 1. Мапінг назв (UI -> Technical)
    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }

    # 2. 🔒 ПЕРЕВІРКА СТАТУСУ (БЛОКУВАННЯ)
    current_proj = st.session_state.get("current_project")
    
    if current_proj is None:
        status = "trial" 
    else:
        status = current_proj.get("status", "trial")
    
    # Якщо статус заблокований або термін дії вийшов - зупиняємо
    if status in ["blocked", "expired"]:
        st.error(f"⛔ Дія недоступна. Ваш статус: {status.upper()}. Будь ласка, зв'яжіться з адміністратором.")
        return False

    try:
        # Отримуємо email безпечно
        user = st.session_state.get("user")
        user_email = user.email if user else "no-reply@virshi.ai"
        
        if isinstance(keywords, str):
            keywords = [keywords]

        # Якщо моделі не обрані або пусті, беремо дефолтну
        if not models:
            models = ["Perplexity"]

        success_count = 0

        # 3. ОТРИМУЄМО ОФІЦІЙНІ ДЖЕРЕЛА (WHITELIST)
        try:
            assets_resp = supabase.table("official_assets")\
                .select("domain_or_url")\
                .eq("project_id", project_id)\
                .execute()
            official_assets = [item["domain_or_url"] for item in assets_resp.data] if assets_resp.data else []
        except Exception as e:
            print(f"Error fetching assets: {e}")
            official_assets = []

        # 🔥 HEADER AUTH
        headers = {
            "virshi-auth": "hi@virshi.ai2025"
        }

        # 4. ЦИКЛ ВІДПРАВКИ
        for ui_model_name in models:
            tech_model_id = MODEL_MAPPING.get(ui_model_name, ui_model_name)

            payload = {
                "project_id": project_id,
                "keywords": keywords, 
                "brand_name": brand_name,
                "user_email": user_email,
                "provider": tech_model_id,
                "models": [tech_model_id],
                "official_assets": official_assets
            }
            
            try:
                # Додано headers=headers
                response = requests.post(
                    N8N_ANALYZE_URL, 
                    json=payload, 
                    headers=headers, 
                    timeout=10
                )
                
                if response.status_code == 200:
                    success_count += 1
                else:
                    st.error(f"Помилка n8n ({ui_model_name}): {response.text}")
                    
            except Exception as inner_e:
                st.error(f"Не вдалося запустити {ui_model_name}: {inner_e}")

        return success_count > 0
            
    except Exception as e:
        st.error(f"Критична помилка запуску: {e}")
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
    """
    Надійний вихід із системи.
    """
    # 1. Видаляємо куку (Token)
    try:
        cookie_manager.delete("virshi_auth_token")
    except Exception:
        pass

    # 2. Виходимо з Supabase (на стороні сервера)
    try:
        supabase.auth.sign_out()
    except Exception:
        pass

    # 3. 🔥 ПОВНЕ очищення Session State
    # Це видаляє всі змінні: user, current_project, налаштування фільтрів тощо.
    st.session_state.clear()

    # 4. Ініціалізуємо критичні змінні, щоб не було помилок до перезавантаження
    st.session_state["user"] = None
    
    # 5. Пауза, щоб браузер встиг фізично видалити куку
    time.sleep(1)

    # 6. Перезавантаження сторінки
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
    """
    Майстер створення першого проекту (2 етапи).
    Оновлено: Стиль карток, редагування, виправлення помилки втрати сесії.
    """
    import requests
    import time
    
    # 🚨 Ініціалізація стейту
    if "onboarding_stage" not in st.session_state:
        st.session_state["onboarding_stage"] = 2
        st.session_state["generated_prompts"] = []
    
    # CSS для стилізації карток та вирівнювання
    st.markdown("""
    <style>
        div[data-testid="stVerticalBlock"] > div[data-testid="stHorizontalBlock"] {
            align-items: center;
        }
        .prompt-card {
            background-color: white;
            padding: 15px;
            border-radius: 8px;
            border: 1px solid #E0E0E0;
            margin-bottom: 10px;
        }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown("## 🚀 Налаштування Проекту")

    # Використовуємо .get для безпеки
    step = st.session_state.get("onboarding_step", 2) 

    with st.container(border=True):

        # ========================================================
        # STEP 2 – дані про бренд (ВВІД)
        # ========================================================
        if step == 2:
            st.subheader("Крок 1: Введіть дані про ваш бренд")

            c1, c2 = st.columns(2)
            with c1:
                brand = st.text_input("Назва бренду", placeholder="Monobank", value=st.session_state.get("temp_brand", ""))
                industry = st.text_input("Галузь бренду / ніша", placeholder="Фінтех, Банкінг", value=st.session_state.get("temp_industry", ""))
                
            with c2:
                domain = st.text_input("Домен (офіційний сайт)", placeholder="monobank.ua", value=st.session_state.get("temp_domain", ""))
                st.markdown("<p style='color: #6c5ce7; margin-top: 10px;'>📍 **Регіон:** UA (Фіксовано)</p>", unsafe_allow_html=True)
            
            products = st.text_area(
                "Продукти / Послуги (перелічіть через кому або у стовпчик)", 
                help="На основі цього буде сформовано запити.",
                value=st.session_state.get("temp_products", "")
            )

            if st.button("Згенерувати запити"):
                if brand and domain and industry and products:
                    # 1. Зберігаємо дані у тимчасовий стейт
                    st.session_state["temp_brand"] = brand
                    st.session_state["temp_domain"] = domain
                    st.session_state["temp_industry"] = industry
                    st.session_state["temp_products"] = products
                    st.session_state["temp_region"] = "UA"

                    with st.spinner("Генеруємо релевантні запити через n8n AI Agent..."):
                        prompts = n8n_generate_prompts(brand, domain, industry, products)
                        
                        if prompts and len(prompts) > 0:
                            st.session_state["generated_prompts"] = prompts
                            st.session_state["onboarding_step"] = 3
                            st.rerun()
                        else:
                            st.error("AI не повернув результатів. Спробуйте змінити опис продуктів.")
                else:
                    st.warning("Будь ласка, заповніть всі 4 поля.")

        # ========================================================
        # STEP 3 – Редагування та Вибір (КОНФІРМАЦІЯ)
        # ========================================================
        elif step == 3:
            # 🛡️ SAFETY CHECK: Перевіряємо, чи не зникли дані сесії
            if not st.session_state.get("temp_brand") or not st.session_state.get("temp_domain"):
                st.warning("⚠️ Дані сесії застаріли. Будь ласка, поверніться на крок назад.")
                if st.button("⬅ Назад до вводу даних"):
                    st.session_state["onboarding_step"] = 2
                    st.rerun()
                return

            st.subheader("Крок 2: Перевірка та редагування запитів")
            st.info("Ви можете відредагувати текст кожного запиту перед запуском. Оберіть галочками ті, що підуть в роботу.")

            prompts_list = st.session_state.get("generated_prompts", [])
            
            if not prompts_list:
                st.error("Помилка: Список запитів порожній.")
                if st.button("Повернутися назад"): 
                    st.session_state["onboarding_step"] = 2
                    st.rerun()
                return

            st.markdown("<br>", unsafe_allow_html=True)
            
            # Словник для збереження обраних (індекс -> текст)
            selected_indices = []

            # --- ЦИКЛ ВИВОДУ КАРТОК (Card Style) ---
            for i, kw in enumerate(prompts_list):
                # Ключ для відстеження режиму редагування
                edit_key = f"edit_mode_row_{i}"
                
                # 🔥 STYLING: Використовуємо container(border=True) для створення ефекту картки
                with st.container(border=True):
                    # Сітка: Чекбокс | Текст | Кнопка
                    c_check, c_text, c_btn = st.columns([0.5, 9, 1])
                    
                    # 1. Чекбокс
                    with c_check:
                        is_checked = st.checkbox("", value=True, key=f"chk_final_{i}", label_visibility="collapsed")
                        if is_checked:
                            selected_indices.append(i)

                    # Перевіряємо режим редагування
                    if st.session_state.get(edit_key, False):
                        # --- РЕЖИМ РЕДАГУВАННЯ ---
                        with c_text:
                            new_val = st.text_input("Редагування", value=kw, key=f"input_kw_{i}", label_visibility="collapsed")
                        
                        with c_btn:
                            # Кнопка Зберегти (Зелена галочка або дискета)
                            if st.button("💾", key=f"save_kw_{i}", help="Зберегти зміни"):
                                st.session_state["generated_prompts"][i] = new_val
                                st.session_state[edit_key] = False
                                st.rerun()
                    else:
                        # --- РЕЖИМ ПЕРЕГЛЯДУ ---
                        with c_text:
                            # Виводимо текст жирним, трохи більшим шрифтом
                            st.markdown(f"<div style='font-size:16px; padding-top:5px;'>{kw}</div>", unsafe_allow_html=True)
                        
                        with c_btn:
                            # Кнопка Редагувати (Олівець)
                            if st.button("✏️", key=f"edit_kw_{i}", help="Редагувати текст"):
                                st.session_state[edit_key] = True
                                st.rerun()

            st.markdown("---")
            
            # Збираємо фінальний список
            final_kws_to_send = [st.session_state["generated_prompts"][idx] for idx in selected_indices]

            c_info, c_action = st.columns([2, 1])
            with c_info:
                st.markdown(f"**Обрано до аналізу:** {len(final_kws_to_send)} запитів")
                st.caption("Натисніть кнопку справа, щоб створити проект і почати.")

            with c_action:
                # --- КНОПКА ЗАПУСКУ ---
                if st.button("🚀 Зберегти та Запустити аналіз", type="primary", use_container_width=True):
                    if len(final_kws_to_send) > 0:
                        with st.spinner("Створення проекту та запуск Gemini..."):
                            try:
                                user_id = st.session_state["user"].id
                                
                                # Беремо дані з сесії (вони вже перевірені на початку кроку)
                                brand_name = st.session_state.get("temp_brand")
                                domain_name = st.session_state.get("temp_domain")
                                region_name = "UA"
                                
                                # 1. Створення проекту в БД
                                res = supabase.table("projects").insert({
                                    "user_id": user_id,
                                    "brand_name": brand_name,
                                    "domain": domain_name,
                                    "region": region_name, 
                                    "status": "trial",
                                }).execute()

                                if not res.data:
                                    raise Exception("Не вдалося створити проект в базі.")

                                proj_data = res.data[0]
                                proj_id = proj_data["id"]

                                # 2. Записуємо ВІДРЕДАГОВАНІ ключові слова
                                kws_data = [
                                    {
                                        "project_id": proj_id, 
                                        "keyword_text": kw_text, 
                                        "is_active": True, 
                                        "is_cron_active": False
                                    } for kw_text in final_kws_to_send
                                ]
                                supabase.table("keywords").insert(kws_data).execute()
                                
                                # 3. Записуємо офіційний домен
                                clean_domain = domain_name.replace("https://", "").replace("http://", "").strip().rstrip("/")
                                supabase.table("official_assets").insert(
                                    {"project_id": proj_id, "domain_or_url": clean_domain, "type": "website"}
                                ).execute()

                                # 4. ВІДПРАВЛЯЄМО НА N8N (Gemini Only)
                                n8n_trigger_analysis(
                                    proj_id, 
                                    final_kws_to_send, 
                                    brand_name,
                                    models=["Google Gemini"] 
                                )

                                # 5. Фінал
                                st.session_state["current_project"] = proj_data
                                st.session_state["onboarding_step"] = 2 
                                st.success("Успіх! Проект створено, аналіз запущено.")
                                time.sleep(2)
                                st.rerun()
                                
                            except Exception as e:
                                st.error(f"Помилка при створенні: {e}")
                    else:
                        st.warning("Оберіть хоча б один запит.")
                    
# =========================
# 6. DASHBOARD
# =========================

def show_competitors_page():
    """
    Сторінка глибокого конкурентного аналізу.
    Оновлено: 
    - Вкладка 'Частота згадки': st.area_chart + таблиця зліва в стилі загального рейтингу.
    """
    import pandas as pd
    import plotly.express as px
    import streamlit as st
    
    # --- 0. ПІДКЛЮЧЕННЯ ---
    if 'supabase' not in globals():
        if 'supabase' in st.session_state:
            supabase = st.session_state['supabase']
        else:
            st.error("🚨 Помилка: Змінна 'supabase' не знайдена.")
            return
    else:
        supabase = globals()['supabase']

    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект.")
        return

    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }

    st.title("👥 Аналіз Конкурентів")

    # --- 1. ЗАВАНТАЖЕННЯ ДАНИХ ---
    try:
        # A. Сканування
        scans_resp = supabase.table("scan_results")\
            .select("id, provider, keyword_id, created_at")\
            .eq("project_id", proj["id"])\
            .execute()
        
        if not scans_resp.data:
            st.info("Даних немає. Запустіть сканування.")
            return
            
        df_scans = pd.DataFrame(scans_resp.data)
        
        # B. Ключові слова
        kw_resp = supabase.table("keywords").select("id, keyword_text").eq("project_id", proj["id"]).execute()
        kw_map = {k['id']: k['keyword_text'] for k in kw_resp.data}
        df_scans['keyword_text'] = df_scans['keyword_id'].map(kw_map)

        # C. Згадки брендів
        scan_ids = df_scans['id'].tolist()
        mentions_resp = supabase.table("brand_mentions")\
            .select("*")\
            .in_("scan_result_id", scan_ids)\
            .execute()
        
        if not mentions_resp.data:
            st.info("Брендів не знайдено.")
            return

        df_mentions = pd.DataFrame(mentions_resp.data)

        # D. Master Data
        df_full = pd.merge(df_mentions, df_scans, left_on='scan_result_id', right_on='id', how='left')

    except Exception as e:
        st.error(f"Помилка обробки даних: {e}")
        return

    # --- 2. ФІЛЬТРИ ---
    with st.container(border=True):
        c1, c2 = st.columns(2)
        with c1:
            all_models = list(MODEL_MAPPING.keys())
            sel_models = st.multiselect("🤖 Фільтр по LLM:", all_models, default=all_models)
            sel_tech_models = [MODEL_MAPPING[m] for m in sel_models]

        with c2:
            all_kws = df_full['keyword_text'].dropna().unique().tolist()
            sel_kws = st.multiselect("🔎 Фільтр по Запитах:", all_kws, default=all_kws)

    # Застосування фільтрів
    if sel_tech_models:
        mask_model = df_full['provider'].apply(lambda x: any(t in str(x) for t in sel_tech_models))
    else:
        mask_model = df_full['provider'].apply(lambda x: False)

    if sel_kws:
        mask_kw = df_full['keyword_text'].isin(sel_kws)
    else:
        mask_kw = df_full['keyword_text'].apply(lambda x: False)

    df_filtered = df_full[mask_model & mask_kw]

    if df_filtered.empty:
        st.warning("За обраними фільтрами даних немає.")
        return

    # --- 3. АГРЕГАЦІЯ ---
    
    # Хелпер: Текст -> Число
    def sentiment_to_score(s):
        if s == 'Позитивний': return 100
        if s == 'Негативний': return 0
        return 50 # Нейтральний
    
    df_filtered['sent_score_num'] = df_filtered['sentiment_score'].apply(sentiment_to_score)

    stats = df_filtered.groupby('brand_name').agg(
        Mentions=('id_x', 'count'),
        Avg_Rank=('rank_position', 'mean'),
        Avg_Sentiment_Num=('sent_score_num', 'mean'),
        Is_My_Brand=('is_my_brand', 'max')
    ).reset_index()

    # Хелпер: Число -> Текст
    def get_sentiment_text(score):
        if score >= 60: return "Позитивна"
        if score <= 40: return "Негативна"
        return "Нейтральна"

    stats['Reputation_Text'] = stats['Avg_Sentiment_Num'].apply(get_sentiment_text)
    stats['Show'] = True 

    # --- 4. ВІДОБРАЖЕННЯ (ВКЛАДКИ) ---
    st.write("") 
    
    tab_list, tab_freq, tab_sent, tab_rank = st.tabs([
        "📋 Детальний рейтинг", 
        "📊 Частота згадки", 
        "⭐ Тональність", 
        "🏆 Середня позиція"
    ])

    # === TAB 1: ДЕТАЛЬНИЙ РЕЙТИНГ ===
    with tab_list:
        st.markdown("##### 📋 Зведена таблиця")
        
        display_df = stats.copy().sort_values('Mentions', ascending=False)
        display_df['Сер. Позиція'] = display_df['Avg_Rank'].apply(lambda x: f"#{x:.1f}")
        display_df['Is_My_Brand'] = display_df['Is_My_Brand'].apply(lambda x: True if x else False)

        st.dataframe(
            display_df[['brand_name', 'Mentions', 'Reputation_Text', 'Сер. Позиція', 'Is_My_Brand']],
            use_container_width=True,
            column_config={
                "brand_name": "Бренд",
                "Mentions": st.column_config.ProgressColumn("Згадок", format="%d", min_value=0, max_value=int(stats['Mentions'].max())),
                "Reputation_Text": st.column_config.TextColumn("Репутація"),
                "Is_My_Brand": st.column_config.CheckboxColumn("Цільовий бренд", disabled=True)
            },
            hide_index=True
        )

    # === TAB 2: ЧАСТОТА ЗГАДКИ (AREA CHART) ===
    with tab_freq:
        st.markdown("##### 📊 Частота згадки (Area Chart)")
        
        col_table, col_chart = st.columns([1.8, 2.2])

        with col_table:
            # Таблиця налаштувань (ідентична по стилю до Tab 1)
            df_freq_editor = stats[['Show', 'brand_name', 'Mentions', 'Is_My_Brand']].copy()
            df_freq_editor = df_freq_editor.sort_values('Mentions', ascending=False)

            edited_freq_df = st.data_editor(
                df_freq_editor,
                column_config={
                    "Show": st.column_config.CheckboxColumn("Show", width="small"),
                    "brand_name": st.column_config.TextColumn("Бренд", disabled=True),
                    "Mentions": st.column_config.ProgressColumn(
                        "Згадок", 
                        format="%d", 
                        min_value=0, 
                        max_value=int(stats['Mentions'].max())
                    ),
                    "Is_My_Brand": st.column_config.CheckboxColumn("Цільовий", disabled=True, width="small")
                },
                hide_index=True,
                use_container_width=True,
                key="editor_freq"
            )

        with col_chart:
            # Дані для графіка
            chart_data = edited_freq_df[edited_freq_df['Show'] == True]
            
            if not chart_data.empty:
                # Готуємо дані для Area Chart (Індекс - Бренд, Значення - Згадки)
                # st.area_chart використовує індекс як вісь X
                chart_view = chart_data.set_index('brand_name')[['Mentions']]
                
                st.markdown("**Динаміка згадок:**")
                st.area_chart(chart_view, color="#00C896")
            else:
                st.info("Оберіть хоча б один бренд у таблиці зліва.")

    # === TAB 3: ТОНАЛЬНІСТЬ (STACKED BAR) ===
    with tab_sent:
        st.markdown("##### ⭐ Аналіз Тональності")
        st.caption("Співвідношення: Позитивні vs Нейтральні vs Негативні.")

        # Агрегація для Stacked Bar
        sent_distribution = df_filtered.groupby(['brand_name', 'sentiment_score']).size().reset_index(name='count')
        total_per_brand = sent_distribution.groupby('brand_name')['count'].transform('sum')
        sent_distribution['percentage'] = (sent_distribution['count'] / total_per_brand * 100).round(1)

        # Керування
        col_list, col_chart = st.columns([1.5, 2.5])
        
        with col_list:
            df_sent_editor = stats[['Show', 'brand_name', 'Reputation_Text']].sort_values('brand_name')
            edited_sent_df = st.data_editor(
                df_sent_editor,
                column_config={
                    "Show": st.column_config.CheckboxColumn("Show", width="small"),
                    "brand_name": "Бренд",
                    "Reputation_Text": "Репутація"
                },
                hide_index=True,
                use_container_width=True,
                key="editor_sent"
            )

        with col_chart:
            selected_brands = edited_sent_df[edited_sent_df['Show'] == True]['brand_name'].tolist()
            chart_data_sent = sent_distribution[sent_distribution['brand_name'].isin(selected_brands)]

            if not chart_data_sent.empty:
                color_map_sent = {
                    "Позитивний": "#00C896",   
                    "Нейтральний": "#E0E0E0",  
                    "Негативний": "#FF4B4B"    
                }
                
                fig_stack = px.bar(
                    chart_data_sent,
                    y="brand_name",
                    x="percentage",
                    color="sentiment_score",
                    orientation='h',
                    text="percentage",
                    color_discrete_map=color_map_sent,
                    category_orders={"sentiment_score": ["Негативний", "Нейтральний", "Позитивний"]},
                    height=500
                )
                
                fig_stack.update_traces(texttemplate='%{text}%', textposition='inside')
                fig_stack.update_layout(
                    barmode='stack',
                    xaxis_title="Частка (%)",
                    yaxis_title="",
                    legend_title="Тональність",
                    plot_bgcolor='rgba(0,0,0,0)',
                    margin=dict(l=0, r=0, t=30, b=0)
                )
                st.plotly_chart(fig_stack, use_container_width=True)
            else:
                st.info("Оберіть бренд для аналізу.")

    # === TAB 4: СЕРЕДНЯ ПОЗИЦІЯ (DONUT INVERSE) ===
    with tab_rank:
        st.markdown("##### 🏆 Середня позиція (Чим менше число - тим краще)")
        
        col_rank_table, col_rank_chart = st.columns([1.5, 2])

        with col_rank_table:
            df_rank_editor = stats[['Show', 'brand_name', 'Avg_Rank', 'Is_My_Brand']].sort_values('Avg_Rank', ascending=True)

            edited_rank_df = st.data_editor(
                df_rank_editor,
                column_config={
                    "Show": st.column_config.CheckboxColumn("Show", width="small"),
                    "brand_name": st.column_config.TextColumn("Бренд", disabled=True),
                    "Avg_Rank": st.column_config.NumberColumn("Ранг", format="%.1f"),
                    "Is_My_Brand": None
                },
                hide_index=True,
                use_container_width=True,
                key="editor_rank"
            )

        with col_rank_chart:
            chart_data_rank = edited_rank_df[edited_rank_df['Show'] == True].copy()
            if not chart_data_rank.empty:
                # Логіка інверсії (для візуального розміру)
                max_rank_val = chart_data_rank['Avg_Rank'].max()
                base_val = max_rank_val + 2 
                chart_data_rank['Inverse_Score'] = base_val - chart_data_rank['Avg_Rank']

                fig_rank = px.pie(
                    chart_data_rank,
                    names='brand_name',
                    values='Inverse_Score', # Розмір сектора
                    hole=0.6,
                    color='Is_My_Brand',
                    color_discrete_map={True: '#00C896', False: '#FFCE56', 1: '#00C896', 0: '#FFCE56'},
                    hover_data=['brand_name']
                )
                # У підписах показуємо РЕАЛЬНИЙ ранг!
                fig_rank.update_traces(
                    customdata=chart_data_rank[['Avg_Rank']],
                    textinfo='label',
                    hovertemplate = "<b>%{label}</b><br>Середнє місце: %{customdata[0]:.1f}"
                )
                
                leader = chart_data_rank.iloc[0]
                fig_rank.update_layout(
                    showlegend=False, 
                    margin=dict(t=20, b=20, l=20, r=20), 
                    height=350,
                    annotations=[dict(text=f"Лідер:<br>{leader['brand_name']}<br>#{leader['Avg_Rank']:.1f}", x=0.5, y=0.5, font_size=14, showarrow=False)]
                )
                st.plotly_chart(fig_rank, use_container_width=True)
            else:
                st.info("Оберіть бренд.")

def show_dashboard():
    import plotly.graph_objects as go
    from datetime import datetime, timedelta, time as dt_time
    
    # Перевірка підключення (Safe Check)
    if 'supabase' not in globals():
        if 'supabase' in st.session_state:
            supabase = st.session_state['supabase']
        else:
            st.error("🚨 Помилка: Змінна 'supabase' не знайдена.")
            return
    else:
        supabase = globals()['supabase']

    proj = st.session_state.get("current_project", {})
    if not proj:
        st.info("Спочатку створіть проект.")
        return

    # --- 1. ВИЗНАЧЕННЯ ПЕРІОДУ ---
    today = datetime.now().date()
    
    try:
        first_scan = supabase.table("scan_results")\
            .select("created_at")\
            .eq("project_id", proj["id"])\
            .order("created_at", desc=False)\
            .limit(1)\
            .execute()
        
        if first_scan.data:
            min_date_str = first_scan.data[0]['created_at']
            min_date = datetime.fromisoformat(min_date_str.replace('Z', '+00:00')).date()
        else:
            min_date = today - timedelta(days=1)
    except Exception:
        min_date = today - timedelta(days=30)

    # ВЕРХНЯ ПАНЕЛЬ
    c_title, c_date = st.columns([3, 1])
    with c_title:
        st.title(f"📊 Дашборд: {proj.get('brand_name')}")
    
    with c_date:
        date_range = st.date_input(
            "Період аналізу:",
            value=(min_date, today),
            min_value=min_date,
            max_value=today,
            format="DD.MM.YYYY"
        )

    st.markdown("---")

    if isinstance(date_range, tuple):
        if len(date_range) == 2:
            start_date, end_date = date_range
        elif len(date_range) == 1:
            start_date = date_range[0]
            end_date = today
        else:
            start_date = min_date
            end_date = today
    else:
        start_date = min_date
        end_date = today

# --- 2. ЗАВАНТАЖЕННЯ ДАНИХ ---
    try:
        # Перетворюємо дати в ISO формат
        iso_start = datetime.combine(start_date, dt_time.min).isoformat()
        iso_end = datetime.combine(end_date, dt_time.max).isoformat()

        # A. ID Сканувань
        scans_query = supabase.table("scan_results")\
            .select("id, created_at, keyword_id")\
            .eq("project_id", proj["id"])\
            .gte("created_at", iso_start)\
            .lte("created_at", iso_end)\
            .execute()
        
        scan_ids = [s['id'] for s in scans_query.data]
        
        # --- ПЕРЕВІРКА НА ПУСТОТУ ---
        if not scan_ids:
            # Тут ми виходимо, якщо немає сканувань взагалі
            st.warning(f"🔍 За період з {start_date.strftime('%d.%m')} по {end_date.strftime('%d.%m')} даних не знайдено.")
            st.info("👉 Запустіть нове сканування.")
            return

        # B. Згадки та Джерела
        mentions_resp = supabase.table("brand_mentions").select("*").in_("scan_result_id", scan_ids).execute()
        sources_resp = supabase.table("extracted_sources").select("*").in_("scan_result_id", scan_ids).execute()
        keywords_resp = supabase.table("keywords").select("id, keyword_text").eq("project_id", proj["id"]).execute()
        
        df_mentions = pd.DataFrame(mentions_resp.data)
        df_sources = pd.DataFrame(sources_resp.data)
        kw_map = {k['id']: k['keyword_text'] for k in keywords_resp.data}
        
        # 🔥 FIX: ЗАХИСТ ВІД KEYERROR - Гарантуємо наявність колонок, навіть якщо відповідь Supabase порожня
        
        # Якщо дані прийшли пустими, створюємо DF з необхідними колонками
        if df_mentions.empty:
            df_mentions = pd.DataFrame(columns=['mention_count', 'is_my_brand', 'sentiment_score', 'rank_position', 'scan_result_id'])
        
        if df_sources.empty:
            df_sources = pd.DataFrame(columns=['is_official', 'domain', 'scan_result_id'])


    except Exception as e:
        st.error(f"Помилка обробки даних: {e}")
        return

    # --- 3. РОЗРАХУНОК МЕТРИК ---
    
    # 1. SOV
    total_mentions = df_mentions['mention_count'].sum() if not df_mentions.empty else 0
    my_mentions = df_mentions[df_mentions['is_my_brand'] == True]['mention_count'].sum() if not df_mentions.empty else 0
    sov = (my_mentions / total_mentions * 100) if total_mentions > 0 else 0.0

    # 2. Офіційні джерела
    total_sources = len(df_sources)
    official_sources = len(df_sources[df_sources['is_official'] == True])
    official_pct = (official_sources / total_sources * 100) if total_sources > 0 else 0.0

    # 3. Sentiment Data
    if not df_mentions.empty:
        my_brand_rows = df_mentions[df_mentions['is_my_brand'] == True].copy()
    else:
        my_brand_rows = pd.DataFrame()

    # 4. Позиція
    if not my_brand_rows.empty:
        found_rows = my_brand_rows[my_brand_rows['rank_position'].notnull()]
        avg_pos = found_rows['rank_position'].mean() if not found_rows.empty else 0
    else:
        avg_pos = 0

    # 5. Присутність
    total_scans_count = len(scan_ids)
    scans_with_me = 0
    if not my_brand_rows.empty:
        scans_with_me = my_brand_rows['scan_result_id'].nunique()
    visibility_rate = (scans_with_me / total_scans_count * 100) if total_scans_count > 0 else 0.0

    # --- 4. ВІЗУАЛІЗАЦІЯ ---
    
    def make_donut(value, color="#00C896"):
        fig = go.Figure(data=[go.Pie(
            values=[value, 100-value], hole=.75,
            marker_colors=[color, "#EEF0F2"], textinfo='none', hoverinfo='none'
        )])
        fig.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), height=120,
            annotations=[dict(text=f"{value:.1f}%", x=0.5, y=0.5, font_size=20, showarrow=False, font_weight='bold')])
        return fig

    st.markdown("""
    <style>
        .dash-card { background-color: white; border: 1px solid #E0E0E0; border-radius: 10px; padding: 20px; text-align: center; height: 280px; display: flex; flex-direction: column; justify-content: space-between; }
        .dash-title { font-size: 12px; text-transform: uppercase; color: #888; font-weight: 600; margin-bottom: 10px; }
    </style>
    """, unsafe_allow_html=True)

    # === РЯДОК 1 (СТВОРЮЄМО КОЛОНКИ ТУТ) ===
    # ⚠️ Ось цей рядок був пропущений, тому виникала помилка
    r1_c1, r1_c2, r1_c3 = st.columns(3)

    # Картка 1: SOV
    with r1_c1:
        with st.container(border=True):
            st.markdown("<div class='dash-title'>ЧАСТКА ГОЛОСУ (SOV)</div>", unsafe_allow_html=True)
            st.plotly_chart(make_donut(sov, "#00C896"), use_container_width=True, key="d_sov")
            st.caption(f"Ви: {int(my_mentions)} | Всього: {int(total_mentions)}")

    # Картка 2: Офіційні джерела
    with r1_c2:
        with st.container(border=True):
            st.markdown("<div class='dash-title'>% ОФІЦІЙНИХ ДЖЕРЕЛ</div>", unsafe_allow_html=True)
            st.plotly_chart(make_donut(official_pct, "#36A2EB"), use_container_width=True, key="d_off")
            st.caption(f"Офіційних: {official_sources}")

    # Картка 3: ЗАГАЛЬНИЙ НАСТРІЙ (НОВИЙ ДИЗАЙН)
    with r1_c3:
        with st.container(border=True):
            st.markdown("<div class='dash-title'>ЗАГАЛЬНИЙ НАСТРІЙ</div>", unsafe_allow_html=True)
            
            # 1. Рахуємо
            if not my_brand_rows.empty:
                s_counts = my_brand_rows['sentiment_score'].value_counts()
                pos_count = s_counts.get('Позитивний', 0)
                neu_count = s_counts.get('Нейтральний', 0)
                neg_count = s_counts.get('Негативний', 0)
                total_sent = pos_count + neu_count + neg_count
            else:
                pos_count = neu_count = neg_count = total_sent = 0

            # 2. Відсотки
            pos_pct = (pos_count / total_sent * 100) if total_sent > 0 else 0
            neu_pct = (neu_count / total_sent * 100) if total_sent > 0 else 0
            neg_pct = (neg_count / total_sent * 100) if total_sent > 0 else 0

            # 3. Графік (Смужка)
            fig_sent = go.Figure()
            fig_sent.add_trace(go.Bar(
                y=[''], x=[pos_pct], name='Pos', orientation='h',
                marker=dict(color='#00C896', line=dict(width=0)),
                hovertemplate='%{x:.1f}%<extra></extra>'
            ))
            fig_sent.add_trace(go.Bar(
                y=[''], x=[neu_pct], name='Neu', orientation='h',
                marker=dict(color='#E0E0E0', line=dict(width=0)),
                hovertemplate='%{x:.1f}%<extra></extra>'
            ))
            fig_sent.add_trace(go.Bar(
                y=[''], x=[neg_pct], name='Neg', orientation='h',
                marker=dict(color='#FF4B4B', line=dict(width=0)),
                hovertemplate='%{x:.1f}%<extra></extra>'
            ))

            fig_sent.update_layout(
                barmode='stack', showlegend=False,
                margin=dict(t=0, b=0, l=0, r=0), height=60,
                xaxis=dict(showgrid=False, showticklabels=False, zeroline=False, range=[0, 100]),
                yaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
                paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
            )
            
            st.plotly_chart(fig_sent, use_container_width=True, config={'displayModeBar': False})

            # 4. Легенда
            st.markdown(f"""
            <div style="display: flex; justify-content: space-between; font-size: 12px; margin-top: 5px;">
                <div style="text-align: center;"><span style="color:#00C896;">●</span> {pos_pct:.0f}%</div>
                <div style="text-align: center;"><span style="color:#999;">●</span> {neu_pct:.0f}%</div>
                <div style="text-align: center;"><span style="color:#FF4B4B;">●</span> {neg_pct:.0f}%</div>
            </div>
            """, unsafe_allow_html=True)
            
            # Висновок
            main_mood = "Даних немає"
            if total_sent > 0:
                if pos_pct >= max(neu_pct, neg_pct): main_mood = "Переважно Позитивний"
                elif neg_pct >= max(pos_pct, neu_pct): main_mood = "Переважно Негативний"
                else: main_mood = "Переважно Нейтральний"
            
            st.markdown(f"<div style='text-align:center; font-weight:bold; margin-top:5px; font-size:14px; color:#333;'>{main_mood}</div>", unsafe_allow_html=True)

    # === РЯДОК 2 ===
    r2_c1, r2_c2, r2_c3 = st.columns(3)

    with r2_c1:
        with st.container(border=True):
            st.markdown("<div class='dash-title'>СЕРЕДНЯ ПОЗИЦІЯ</div>", unsafe_allow_html=True)
            val_display = f"{avg_pos:.1f}" if avg_pos > 0 else "-"
            st.markdown(f"<div style='text-align:center; font-size: 48px; font-weight: bold; color: #00C896; margin-top: 30px;'>{val_display}</div>", unsafe_allow_html=True)
            
            fig_gauge = go.Figure(go.Indicator(
                mode = "gauge",
                value = avg_pos if avg_pos > 0 else 0,
                domain = {'x': [0, 1], 'y': [0, 1]},
                gauge = {
                    'axis': {'range': [10, 1], 'visible': False}, 
                    'bar': {'color': "#00C896"},
                    'bgcolor': "white",
                }
            ))
            fig_gauge.update_layout(height=100, margin=dict(t=0,b=0,l=20,r=20))
            st.plotly_chart(fig_gauge, use_container_width=True, key="d_pos")

    with r2_c2:
        with st.container(border=True):
            st.markdown("<div class='dash-title'>ПРИСУТНІСТЬ БРЕНДУ</div>", unsafe_allow_html=True)
            st.plotly_chart(make_donut(visibility_rate, "#9966FF"), use_container_width=True, key="d_vis")
            st.caption(f"Знайдено у {scans_with_me} з {total_scans_count} сканувань")

    with r2_c3:
        domain_mentions = 0
        if not df_sources.empty:
            domain_mentions = len(df_sources[df_sources['domain'].str.contains(proj.get('domain', 'MISSING'), na=False, case=False)])
        domain_pct = (domain_mentions / total_sources * 100) if total_sources > 0 else 0.0
        
        with st.container(border=True):
            st.markdown("<div class='dash-title'>ЗГАДКИ ДОМЕНУ</div>", unsafe_allow_html=True)
            st.plotly_chart(make_donut(domain_pct, "#FF9F40"), use_container_width=True, key="d_dom")
            st.caption(f"{domain_mentions} прямих посилань")

    st.markdown("---")

    # 5. ТАБЛИЦЯ
    st.subheader("📋 Деталізація за період")
    
    latest_scans_df = pd.DataFrame(scans_query.data)
    if not latest_scans_df.empty:
        latest_scans_df = latest_scans_df.sort_values('created_at', ascending=False).drop_duplicates('keyword_id')
        
        table_rows = []
        for index, row in latest_scans_df.iterrows():
            kw_text = kw_map.get(row['keyword_id'], "—")
            
            scan_mentions = df_mentions[df_mentions['scan_result_id'] == row['id']]
            my_mention = scan_mentions[scan_mentions['is_my_brand'] == True]
            
            if not my_mention.empty:
                pos = my_mention.iloc[0]['rank_position']
                sent = my_mention.iloc[0]['sentiment_score']
                is_present = True
            else:
                pos = None
                sent = "Не знайдено"
                is_present = False
                
            table_rows.append({
                "Запит": kw_text,
                "Дата": datetime.fromisoformat(row['created_at']).strftime("%d.%m"),
                "Позиція": pos if pos else "-",
                "Тональність": sent,
                "Знайдено?": is_present
            })
            
        st.dataframe(
            pd.DataFrame(table_rows),
            use_container_width=True,
            column_config={
                "Знайдено?": st.column_config.CheckboxColumn("Знайдено?", disabled=True),
            },
            hide_index=True
        )

# =========================
# 7. КЕРУВАННЯ ЗАПИТАМИ
# =========================

def show_keyword_details(kw_id):
    """
    Сторінка детальної аналітики одного запиту.
    ВЕРСІЯ: FINAL FIXED (OPENAI TAB FIX).
    1. Fix OpenAI Tab: Фільтрація тепер йде по 'provider_ui', а не по точному 'provider'.
       Це вирішує проблему, якщо в базі записано 'gpt-4o', а скрипт шукав щось інше.
    2. Всі попередні фікси (метрики, видалення, таймзона) збережені.
    """
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    import streamlit as st
    from datetime import datetime, timedelta
    import numpy as np
    import time
    import re
    
    # 0. ПІДКЛЮЧЕННЯ
    if 'supabase' not in globals():
        if 'supabase' in st.session_state:
            supabase = st.session_state['supabase']
        else:
            st.error("🚨 Помилка: Змінна 'supabase' не знайдена.")
            return
    else:
        supabase = globals()['supabase']

    # --- MAPPING ---
    # Ключі тут — це назви вкладок (UI)
    MODEL_CONFIG = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }
    ALL_MODELS_UI = list(MODEL_CONFIG.keys())
    
    # Функція нормалізації назв з бази
    def get_ui_model_name(db_name):
        # 1. Точний збіг
        for ui, db in MODEL_CONFIG.items():
            if db == db_name: return ui
        
        # 2. Нечіткий пошук (Fallback)
        lower = str(db_name).lower()
        if "perplexity" in lower: return "Perplexity"
        if "gpt" in lower or "openai" in lower: return "OpenAI GPT"
        if "gemini" in lower or "google" in lower: return "Google Gemini"
        
        return db_name # Якщо не знайшли, повертаємо як є

    def tooltip(text):
        return f'<span title="{text}" style="cursor:help; font-size:14px; color:#333; margin-left:4px;">ℹ️</span>'

    def normalize_url(u):
        u = str(u).strip()
        u = re.split(r'[)\]]', u)[0] # Очистка від Markdown
        if not u.startswith(('http://', 'https://')): return f"https://{u}"
        return u

    # 1. ОТРИМАННЯ ДАНИХ ЗАПИТУ
    try:
        kw_resp = supabase.table("keywords").select("*").eq("id", kw_id).execute()
        if not kw_resp.data:
            st.error("Запит не знайдено.")
            st.session_state["focus_keyword_id"] = None
            st.rerun()
            return
        
        keyword_record = kw_resp.data[0]
        keyword_text = keyword_record["keyword_text"]
        project_id = keyword_record["project_id"]
        
        proj = st.session_state.get("current_project", {})
        target_brand_name = proj.get("brand_name", "").strip()
        
    except Exception as e:
        st.error(f"Помилка БД: {e}")
        return

    # HEADER
    col_back, col_title = st.columns([1, 15])
    with col_back:
        if st.button("⬅", key="back_from_details", help="Назад до списку"):
            st.session_state["focus_keyword_id"] = None
            st.rerun()
    
    with col_title:
        st.markdown(f"<h3 style='margin-top: -5px;'>🔍 {keyword_text}</h3>", unsafe_allow_html=True)

    # ⚙️ БЛОК НАЛАШТУВАНЬ
    with st.expander("⚙️ Налаштування та Нове сканування", expanded=False):
        c1, c2 = st.columns(2)
        
        # ЛІВА: РЕДАГУВАННЯ
        with c1:
            edit_key = f"edit_mode_{kw_id}"
            if edit_key not in st.session_state: st.session_state[edit_key] = False
            
            new_text = st.text_input(
                "Текст запиту", 
                value=keyword_text, 
                key="edit_kw_input",
                disabled=not st.session_state[edit_key]
            )
            
            if not st.session_state[edit_key]:
                if st.button("✏️ Редагувати", key="enable_edit_btn"):
                    st.session_state[edit_key] = True
                    st.rerun()
            else:
                if st.button("💾 Зберегти", key="save_kw_btn"):
                    if new_text and new_text != keyword_text:
                        supabase.table("keywords").update({"keyword_text": new_text}).eq("id", kw_id).execute()
                        st.success("Збережено!")
                    st.session_state[edit_key] = False
                    st.rerun()

        # ПРАВА: ЗАПУСК
        with c2:
            selected_models_to_run = st.multiselect(
                "Оберіть моделі для сканування:", 
                options=ALL_MODELS_UI, 
                default=ALL_MODELS_UI, 
                key="rescan_models_select"
            )
            
            confirm_run_key = f"confirm_run_{kw_id}"
            if confirm_run_key not in st.session_state: st.session_state[confirm_run_key] = False

            if not st.session_state[confirm_run_key]:
                if st.button("🚀 Запустити сканування", key="pre_run_btn"):
                    st.session_state[confirm_run_key] = True
                    st.rerun()
            else:
                c_conf1, c_conf2 = st.columns(2)
                with c_conf1:
                    if st.button("✅ Підтвердити", type="primary", key="real_run_btn"):
                        proj = st.session_state.get("current_project", {})
                        if 'n8n_trigger_analysis' in globals():
                            n8n_trigger_analysis(
                                project_id, 
                                [new_text], 
                                proj.get("brand_name"), 
                                models=selected_models_to_run
                            )
                            st.success("Задачу відправлено! Оновлення даних...")
                            time.sleep(2)
                            st.session_state[confirm_run_key] = False
                            st.rerun()
                        else:
                            st.error("Функція запуску не знайдена.")
                with c_conf2:
                    if st.button("❌ Скасувати", key="cancel_run_btn"):
                        st.session_state[confirm_run_key] = False
                        st.rerun()

    # 2. ОТРИМАННЯ ДАНИХ
    try:
        scans_resp = supabase.table("scan_results")\
            .select("id, created_at, provider, raw_response")\
            .eq("keyword_id", kw_id)\
            .order("created_at", desc=False)\
            .execute()
        
        scans_data = scans_resp.data if scans_resp.data else []
        df_scans = pd.DataFrame(scans_data)
        
        if not df_scans.empty:
            df_scans.rename(columns={'id': 'scan_id'}, inplace=True)
            
            # --- TIMEZONE FIX (Kyiv) ---
            df_scans['created_at'] = pd.to_datetime(df_scans['created_at'])
            if df_scans['created_at'].dt.tz is None:
                df_scans['created_at'] = df_scans['created_at'].dt.tz_localize('UTC')
            df_scans['created_at'] = df_scans['created_at'].dt.tz_convert('Europe/Kiev')
            df_scans['date_str'] = df_scans['created_at'].dt.strftime('%Y-%m-%d %H:%M')
            
            # 🔥 Нормалізація назви провайдера (GPT-4o -> OpenAI GPT)
            df_scans['provider_ui'] = df_scans['provider'].apply(get_ui_model_name)
        else:
            df_scans = pd.DataFrame(columns=['scan_id', 'created_at', 'provider', 'raw_response', 'date_str', 'provider_ui'])

        # B. Mentions
        if not df_scans.empty:
            scan_ids = df_scans['scan_id'].tolist()
            if scan_ids:
                mentions_resp = supabase.table("brand_mentions")\
                    .select("*")\
                    .in_("scan_result_id", scan_ids)\
                    .execute()
                mentions_data = mentions_resp.data if mentions_resp.data else []
                df_mentions = pd.DataFrame(mentions_data)
            else:
                df_mentions = pd.DataFrame()
        else:
            df_mentions = pd.DataFrame()

        # SMART MERGE (Дублікати)
        if not df_mentions.empty and target_brand_name:
            df_mentions['brand_clean'] = df_mentions['brand_name'].astype(str).str.lower().str.strip()
            target_norm = target_brand_name.lower().split(' ')[0]
            mask_match = df_mentions['brand_clean'].str.contains(target_norm, na=False)
            df_mentions['is_real_target'] = mask_match | (df_mentions['is_my_brand'] == True)
        elif not df_mentions.empty:
            df_mentions['is_real_target'] = df_mentions['is_my_brand']

        # C. Merge
        if not df_mentions.empty:
            df_full = pd.merge(df_scans, df_mentions, left_on='scan_id', right_on='scan_result_id', how='left')
        else:
            df_full = df_scans.copy()
            df_full['mention_count'] = 0
            df_full['is_real_target'] = False
            df_full['scan_result_id'] = df_full['scan_id'] if not df_full.empty else None
            df_full['sentiment_score'] = None
            df_full['rank_position'] = None
            df_full['brand_name'] = None

    except Exception as e:
        st.error(f"Помилка обробки даних: {e}")
        return

    # 3. KPI (GLOBAL)
    if not df_mentions.empty:
        my_brand_data = df_mentions[df_mentions['is_real_target'] == True]
        
        total_my_mentions = my_brand_data['mention_count'].sum()
        unique_competitors = df_mentions[df_mentions['is_real_target'] == False]['brand_name'].nunique()
        
        scan_totals = df_mentions.groupby('scan_result_id')['mention_count'].sum().reset_index()
        scan_totals.rename(columns={'mention_count': 'scan_total'}, inplace=True)
        
        my_mentions_per_scan = my_brand_data.groupby('scan_result_id')['mention_count'].sum().reset_index()
        my_mentions_per_scan.rename(columns={'mention_count': 'my_count'}, inplace=True)
        
        sov_df = pd.merge(scan_totals, my_mentions_per_scan, on='scan_result_id', how='left')
        sov_df['my_count'] = sov_df['my_count'].fillna(0)
        
        mask_nonzero = sov_df['scan_total'] > 0
        sov_df.loc[mask_nonzero, 'sov'] = (sov_df.loc[mask_nonzero, 'my_count'] / sov_df.loc[mask_nonzero, 'scan_total']) * 100
        avg_sov = sov_df['sov'].mean() if not sov_df.empty else 0
        
        valid_ranks = my_brand_data[my_brand_data['rank_position'] > 0]['rank_position']
        avg_pos = valid_ranks.mean()
        display_pos = f"#{avg_pos:.1f}" if pd.notna(avg_pos) else "-"
        
        if not my_brand_data.empty:
            active_mentions = my_brand_data[my_brand_data['mention_count'] > 0]
            if not active_mentions.empty:
                s_counts = active_mentions['sentiment_score'].value_counts()
                total_s = s_counts.sum()
                pos_pct = (s_counts.get("Позитивний", 0) / total_s) * 100
                neg_pct = (s_counts.get("Негативний", 0) / total_s) * 100
                neu_pct = (s_counts.get("Нейтральний", 0) / total_s) * 100
            else:
                pos_pct, neg_pct, neu_pct = 0, 0, 0
        else:
            pos_pct, neg_pct, neu_pct = 0, 0, 0
    else:
        avg_sov, total_my_mentions, unique_competitors = 0, 0, 0
        display_pos = "-"
        pos_pct, neg_pct, neu_pct = 0, 0, 0

    st.markdown("""
    <style>
        .stat-box {
            background-color: #ffffff;
            border: 1px solid #E0E0E0;
            border-top: 4px solid #8041F6; 
            border-radius: 8px;
            padding: 15px;
            text-align: center;
            box-shadow: 0 4px 10px rgba(0,0,0,0.03);
            height: 140px;
            display: flex; flex-direction: column; justify-content: center;
        }
        .stat-label { font-size: 11px; color: #888; text-transform: uppercase; font-weight: 600; margin-bottom: 5px; }
        .stat-value { font-size: 26px; font-weight: 700; color: #333; line-height: 1.2;}
        .stat-sub { font-size: 13px; color: #666; margin-top: 4px; }
    </style>
    """, unsafe_allow_html=True)

    if total_my_mentions > 0:
        sent_display = f"""
        <span style='color:#00C896'>😊 {pos_pct:.0f}%</span> &nbsp;
        <span style='color:#FFCE56'>😐 {neu_pct:.0f}%</span> &nbsp;
        <span style='color:#FF4B4B'>😡 {neg_pct:.0f}%</span>
        """
    else:
        sent_display = "<span style='color:#999'>Не згадано</span>"

    k1, k2, k3, k4 = st.columns(4)
    with k1:
        tt = tooltip("Частка голосу (SOV) — % згадок вашого бренду.")
        st.markdown(f"""<div class="stat-box"><div class="stat-label">Частка голосу (SOV) {tt}</div><div class="stat-value">{avg_sov:.1f}%</div></div>""", unsafe_allow_html=True)
    with k2:
        tt = tooltip("Всього згадок вашого бренду (та кількість унікальних брендів конкурентів).")
        st.markdown(f"""<div class="stat-box"><div class="stat-label">Згадок (Всього) {tt}</div><div class="stat-value">{int(total_my_mentions)}</div><div class="stat-sub">Конкурентів: {unique_competitors}</div></div>""", unsafe_allow_html=True)
    with k3:
        tt = tooltip("Розподіл тональності (Позитив / Нейтраль / Негатив).")
        st.markdown(f"""<div class="stat-box"><div class="stat-label">Тональність {tt}</div><div style="font-size: 14px; font-weight:600; margin-top:10px;">{sent_display}</div></div>""", unsafe_allow_html=True)
    with k4:
        tt = tooltip("Середня позиція у списку (якщо бренд знайдено).")
        st.markdown(f"""<div class="stat-box"><div class="stat-label">Сер. Позиція {tt}</div><div class="stat-value">{display_pos}</div></div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # 4. ГРАФІК ДИНАМІКИ
    st.markdown("##### 📈 Динаміка показників")

    if not df_full.empty and 'scan_id' in df_full.columns:
        totals = df_full.groupby('scan_id')['mention_count'].sum().reset_index()
        totals.rename(columns={'mention_count': 'scan_total'}, inplace=True)
        df_plot_base = pd.merge(df_full, totals, on='scan_id', how='left')
        df_plot_base['sov'] = (df_plot_base['mention_count'] / df_plot_base['scan_total'] * 100).fillna(0)
    else:
        df_plot_base = pd.DataFrame()

    with st.container(border=True):
        f_col1, f_col2, f_col3 = st.columns([1.2, 1.2, 2.5])
        with f_col1:
            metric_choice = st.selectbox("Метрика:", ["Частка голосу (SOV)", "Згадки бренду", "Позиція у списку"])
        with f_col2:
            if not df_plot_base.empty:
                min_d = df_plot_base['created_at'].min().date()
                max_d = df_plot_base['created_at'].max().date()
                date_range = st.date_input("Діапазон дат:", value=(min_d, max_d), min_value=min_d, max_value=max_d)
            else:
                date_range = None
                st.date_input("Діапазон дат:", disabled=True)
        with f_col3:
            col_llm, col_brand = st.columns(2)
            with col_llm:
                selected_llm_ui = st.multiselect("Фільтр по LLM:", options=ALL_MODELS_UI, default=ALL_MODELS_UI)
            with col_brand:
                if not df_plot_base.empty:
                    all_found_brands = sorted([str(b) for b in df_plot_base['brand_name'].unique() if pd.notna(b)])
                    proj = st.session_state.get("current_project", {})
                    my_brand_name = proj.get("brand_name", "")
                    default_sel = [my_brand_name] if my_brand_name in all_found_brands else ([all_found_brands[0]] if all_found_brands else [])
                    selected_brands = st.multiselect("Фільтр по Брендах:", options=all_found_brands, default=default_sel)
                else:
                    st.multiselect("Фільтр по Брендах:", options=[], disabled=True)

    if not df_plot_base.empty and date_range:
        if isinstance(date_range, tuple):
            if len(date_range) == 2:
                start_d, end_d = date_range
                mask_date = (df_plot_base['created_at'].dt.date >= start_d) & (df_plot_base['created_at'].dt.date <= end_d)
                df_plot_base = df_plot_base[mask_date]
            elif len(date_range) == 1:
                start_d = date_range[0]
                mask_date = (df_plot_base['created_at'].dt.date == start_d)
                df_plot_base = df_plot_base[mask_date]

        df_plot_base = df_plot_base[df_plot_base['provider_ui'].isin(selected_llm_ui)]
        if 'selected_brands' in locals() and selected_brands:
            df_plot_base = df_plot_base[df_plot_base['brand_name'].isin(selected_brands)]
        
        df_plot_base = df_plot_base.sort_values('created_at')

        if not df_plot_base.empty:
            if metric_choice == "Частка голосу (SOV)":
                y_col = "sov"
                y_title = "SOV (%)"
                y_range = [0, 100]
            elif metric_choice == "Згадки бренду":
                y_col = "mention_count"
                y_title = "Кількість згадок"
                y_range = None
            else:
                y_col = "rank_position"
                y_title = "Позиція"
                y_range = None

            df_plot_base['legend_label'] = df_plot_base['brand_name'] + " (" + df_plot_base['provider_ui'] + ")"

            fig = px.line(
                df_plot_base, 
                x="created_at", 
                y=y_col, 
                color="legend_label",
                markers=True,
                labels={"created_at": "Час", "legend_label": "Легенда", y_col: y_title}
            )
            
            if y_range: fig.update_yaxes(range=y_range)
            if metric_choice == "Позиція у списку": fig.update_yaxes(autorange="reversed")

            fig.update_xaxes(showgrid=True, showticklabels=True, tickformat="%d.%m\n%H:%M", title_text="Час")
            fig.update_layout(height=350, hovermode="x unified", margin=dict(l=0, r=0, t=10, b=0), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        else:
            st.info("Немає даних за обраними критеріями.")
    else:
        st.info("Історія сканувань порожня або не обрано дати.")

    st.markdown("---")

    # 5. ДЕТАЛІЗАЦІЯ (TABS)
    st.markdown("##### 📝 Детальний аналіз відповідей")
    
    tabs = st.tabs(ALL_MODELS_UI)
    
    for tab, ui_model_name in zip(tabs, ALL_MODELS_UI):
        with tab:
            # 🔥 FIX: Фільтрація по 'provider_ui' (нормалізоване ім'я)
            if not df_scans.empty:
                model_scans = df_scans[df_scans['provider_ui'] == ui_model_name].sort_values('created_at', ascending=False)
            else:
                model_scans = pd.DataFrame()
            
            if model_scans.empty:
                st.write(f"📉 Даних від **{ui_model_name}** ще немає.")
                continue

            with st.container(border=True):
                scan_options = {row['date_str']: row['scan_id'] for _, row in model_scans.iterrows()}
                
                c_sel, c_del = st.columns([3, 1])
                with c_sel:
                    selected_date = st.selectbox(
                        f"Оберіть дату аналізу ({ui_model_name}):", 
                        list(scan_options.keys()), 
                        key=f"sel_date_{ui_model_name}" # унікальний ключ
                    )
                
                selected_scan_id = scan_options[selected_date]
                
                with c_del:
                    st.write("") 
                    st.write("")
                    confirm_key = f"del_scan_{selected_scan_id}"
                    if confirm_key not in st.session_state: st.session_state[confirm_key] = False

                    if not st.session_state[confirm_key]:
                        if st.button("🗑️ Видалити", key=f"btn_del_{selected_scan_id}"):
                            st.session_state[confirm_key] = True
                            st.rerun()
                    else:
                        c_y, c_n = st.columns(2)
                        if c_y.button("✅", key=f"yes_{selected_scan_id}"):
                            try:
                                supabase.table("scan_results").delete().eq("id", selected_scan_id).execute()
                                st.success("Видалено!")
                                st.session_state[confirm_key] = False
                                time.sleep(1)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Помилка: {e}")
                        
                        if c_n.button("❌", key=f"no_{selected_scan_id}"):
                            st.session_state[confirm_key] = False
                            st.rerun()

            current_scan_row = model_scans[model_scans['scan_id'] == selected_scan_id].iloc[0]
            
            # --- LOCAL METRICS ---
            loc_sov = 0
            loc_mentions = 0
            loc_sent = "Не згадано"
            loc_rank_str = "-"
            
            current_scan_mentions = pd.DataFrame()
            if not df_mentions.empty:
                current_scan_mentions = df_mentions[df_mentions['scan_result_id'] == selected_scan_id]
            
            if not current_scan_mentions.empty:
                total_in_scan = current_scan_mentions['mention_count'].sum()
                
                # 🔥 FIX: Фільтруємо ВСІ рядки, що підходять під "Мій Бренд"
                my_brand_rows = current_scan_mentions[current_scan_mentions['is_real_target'] == True]

                if not my_brand_rows.empty:
                    val_my_mentions = my_brand_rows['mention_count'].sum()
                    valid_ranks = my_brand_rows[my_brand_rows['rank_position'] > 0]['rank_position']
                    val_rank = valid_ranks.min() if not valid_ranks.empty else None
                    
                    if val_my_mentions > 0:
                        main_row = my_brand_rows.sort_values('mention_count', ascending=False).iloc[0]
                        loc_sent = main_row['sentiment_score']
                    
                    loc_mentions = int(val_my_mentions)
                    loc_sov = (val_my_mentions / total_in_scan * 100) if total_in_scan > 0 else 0
                    loc_rank_str = f"#{val_rank:.0f}" if pd.notna(val_rank) else "-"
            
            sent_color = "#333"
            if loc_sent == "Позитивний": sent_color = "#00C896"
            elif loc_sent == "Негативний": sent_color = "#FF4B4B"
            elif loc_sent == "Не знайдено": sent_color = "#999"

            st.markdown(f"""
            <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 20px;">
                <div style="background:#fff; border:1px solid #E0E0E0; border-top:4px solid #00C896; border-radius:8px; padding:15px; text-align:center;">
                    <div style="font-size:11px; color:#888; font-weight:600;">ЧАСТКА ГОЛОСУ (SOV) {tooltip('Відсоток згадок вашого бренду в цій конкретній відповіді.')}</div>
                    <div style="font-size:24px; font-weight:700; color:#333;">{loc_sov:.1f}%</div>
                </div>
                <div style="background:#fff; border:1px solid #E0E0E0; border-top:4px solid #00C896; border-radius:8px; padding:15px; text-align:center;">
                    <div style="font-size:11px; color:#888; font-weight:600;">ЗГАДОК БРЕНДУ {tooltip('Кількість разів, коли бренд був згаданий.')}</div>
                    <div style="font-size:24px; font-weight:700; color:#333;">{loc_mentions}</div>
                </div>
                <div style="background:#fff; border:1px solid #E0E0E0; border-top:4px solid #00C896; border-radius:8px; padding:15px; text-align:center;">
                    <div style="font-size:11px; color:#888; font-weight:600;">ТОНАЛЬНІСТЬ {tooltip('Емоційне забарвлення згадки в цій відповіді.')}</div>
                    <div style="font-size:18px; font-weight:600; color:{sent_color}; margin-top:5px;">{loc_sent}</div>
                </div>
                <div style="background:#fff; border:1px solid #E0E0E0; border-top:4px solid #00C896; border-radius:8px; padding:15px; text-align:center;">
                    <div style="font-size:11px; color:#888; font-weight:600;">ПОЗИЦІЯ У СПИСКУ {tooltip('Порядковий номер першої згадки бренду.')}</div>
                    <div style="font-size:24px; font-weight:700; color:#333;">{loc_rank_str}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            raw_text = current_scan_row.get('raw_response', '')
            st.markdown("##### Відповідь від LLM")
            proj = st.session_state.get("current_project", {})
            brand_name = proj.get("brand_name", "")
            
            if raw_text:
                final_html = raw_text
                if brand_name:
                    highlight_span = f"<span style='background-color:#dcfce7; color:#166534; font-weight:bold; padding:0 4px; border-radius:4px;'>{brand_name}</span>"
                    final_html = final_html.replace(brand_name, highlight_span)
                st.markdown(f"""<div style="background-color: #f9fffb; border: 1px solid #bbf7d0; border-radius: 8px; padding: 20px; font-size: 16px; line-height: 1.6; color: #374151;">{final_html}</div>""", unsafe_allow_html=True)
            else:
                st.info("Текст відповіді не збережено.")

            st.markdown("<br>", unsafe_allow_html=True)

            # --- БРЕНДИ (Center Layout) ---
            st.markdown(f"**Знайдені бренди:** {tooltip('Бренди, які AI згадав у цій відповіді.')}", unsafe_allow_html=True)
            
            if not current_scan_mentions.empty:
                scan_mentions_plot = current_scan_mentions[current_scan_mentions['mention_count'] > 0].copy()
                scan_mentions_plot = scan_mentions_plot.sort_values('mention_count', ascending=False)

                if not scan_mentions_plot.empty:
                    c_chart, c_table = st.columns([1.3, 2], vertical_alignment="center")
                    with c_chart:
                        fig_brands = px.pie(
                            scan_mentions_plot, values='mention_count', names='brand_name', hole=0.5,
                            color_discrete_sequence=px.colors.qualitative.Pastel,
                            labels={'brand_name': 'Бренд', 'mention_count': 'Згадок'}
                        )
                        fig_brands.update_traces(textposition='inside', textinfo='percent+label', hovertemplate='<b>%{label}</b><br>Згадок: %{value}')
                        fig_brands.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), height=250)
                        st.plotly_chart(fig_brands, use_container_width=True, config={'displayModeBar': False})
                    with c_table:
                        st.dataframe(
                            scan_mentions_plot[['brand_name', 'mention_count', 'rank_position', 'sentiment_score']],
                            column_config={
                                "brand_name": "Бренд",
                                "mention_count": st.column_config.NumberColumn("Згадок"),
                                "rank_position": st.column_config.NumberColumn("Позиція"),
                                "sentiment_score": st.column_config.TextColumn("Тональність")
                            },
                            use_container_width=True, hide_index=True
                        )
                else:
                     st.info("Брендів не знайдено.")
            else:
                st.info("Брендів не знайдено.")
            
            st.markdown("<br>", unsafe_allow_html=True)

            # --- ДЖЕРЕЛА (FIXED: Grouped + Center + Count) ---
            st.markdown(f"#### 🔗 Цитовані джерела {tooltip('Посилання, які надала модель.')}", unsafe_allow_html=True)
            try:
                sources_resp = supabase.table("extracted_sources").select("*").eq("scan_result_id", selected_scan_id).execute()
                sources_data = sources_resp.data
                if sources_data:
                    df_src = pd.DataFrame(sources_data)
                    
                    if 'url' in df_src.columns:
                        if 'domain' not in df_src.columns:
                            df_src['domain'] = df_src['url'].apply(lambda x: str(x).split('/')[2] if x and '//' in str(x) else 'unknown')
                        
                        df_src['url'] = df_src['url'].apply(normalize_url)
                        
                        if 'is_official' in df_src.columns:
                            df_src['status_text'] = df_src['is_official'].apply(lambda x: "✅ Офіційне" if x is True else "🔗 Зовнішнє")
                        else:
                            df_src['status_text'] = "🔗 Зовнішнє"

                        # ГРУПУВАННЯ
                        df_grouped_src = df_src.groupby(['url', 'domain', 'status_text'], as_index=False).size()
                        df_grouped_src = df_grouped_src.rename(columns={'size': 'count'})
                        df_grouped_src = df_grouped_src.sort_values(by='count', ascending=False)

                        c_src_chart, c_src_table = st.columns([1.3, 2], vertical_alignment="center")
                        
                        with c_src_chart:
                            domain_counts = df_grouped_src.groupby('domain')['count'].sum().reset_index()
                            fig_src = px.pie(
                                domain_counts.head(10), values='count', names='domain', hole=0.5,
                                labels={'domain': 'Домен', 'count': 'Кількість'}
                            )
                            fig_src.update_traces(textposition='inside', textinfo='percent', hovertemplate='<b>%{label}</b><br>Кількість: %{value}')
                            fig_src.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), height=200)
                            st.plotly_chart(fig_src, use_container_width=True, config={'displayModeBar': False})

                        with c_src_table:
                            st.dataframe(
                                df_grouped_src[['url', 'status_text', 'count']], 
                                use_container_width=True, 
                                hide_index=True,
                                column_config={
                                    "url": st.column_config.LinkColumn("Посилання", width="large", validate="^https?://"),
                                    "status_text": st.column_config.TextColumn("Тип", width="small"),
                                    "count": st.column_config.NumberColumn("К-сть", width="small")
                                }
                            )
                    else:
                        st.info("URL не знайдено.")
                else:
                    st.info("ℹ️ Джерел не знайдено.")
            except Exception as e:
                st.error(f"Помилка завантаження джерел: {e}")



def show_keywords_page():
    """
    Сторінка списку запитів.
    ВЕРСІЯ: FINAL UI (NO BG ON NAME, PRIMARY ANALYZE BUTTON).
    """
    import pandas as pd
    import streamlit as st
    from datetime import datetime
    import time
    
    # CSS Стилізація
    st.markdown("""
    <style>
        /* 1. Зелені номери */
        .green-number {
            background-color: #00C896;
            color: white;
            width: 28px;
            height: 28px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: bold;
            font-size: 14px;
            margin-top: 5px; 
        }
        
        /* 2. Назва запиту (3-й стовпчик): ПРИБИРАЄМО ФОН */
        div[data-testid="stColumn"]:nth-of-type(3) button[kind="secondary"] {
            border: none;
            background: transparent;
            text-align: left;
            padding-left: 0;
            font-weight: 600;
            color: #31333F;
            box-shadow: none;
        }
        /* Ефект наведення - тільки колір тексту */
        div[data-testid="stColumn"]:nth-of-type(3) button[kind="secondary"]:hover {
            color: #00C896;
            background: transparent;
            border: none;
            box-shadow: none;
        }
        div[data-testid="stColumn"]:nth-of-type(3) button[kind="secondary"]:active {
            color: #00C896;
            background: transparent;
            box-shadow: none;
        }

        /* 3. Інші кнопки (Видалити/Деталі) залишаються стандартними */
    </style>
    """, unsafe_allow_html=True)

    # Таймзони
    try:
        import pytz
        kyiv_tz = pytz.timezone('Europe/Kiev')
    except ImportError:
        kyiv_tz = None

    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }

    if 'supabase' not in globals():
        if 'supabase' in st.session_state:
            supabase = st.session_state['supabase']
        else:
            st.error("🚨 Помилка: Змінна 'supabase' не знайдена.")
            return
    else:
        supabase = globals()['supabase']

    if "kw_input_count" not in st.session_state:
        st.session_state["kw_input_count"] = 1

    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект в онбордингу.")
        return

    # Перехід на деталі
    if st.session_state.get("focus_keyword_id"):
        show_keyword_details(st.session_state["focus_keyword_id"])
        return

    # --- 1. ЗАГОЛОВОК (Зменшений) ---
    st.markdown("<h3 style='padding-top:0;'>📋 Перелік запитів</h3>", unsafe_allow_html=True)

    # Хелпери
    def format_kyiv_time(iso_str):
        if not iso_str or iso_str == "1970-01-01T00:00:00+00:00":
            return "—"
        try:
            dt_utc = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
            if kyiv_tz:
                dt_kyiv = dt_utc.astimezone(kyiv_tz)
                return dt_kyiv.strftime("%d.%m %H:%M")
            else:
                return dt_utc.strftime("%d.%m %H:%M UTC")
        except:
            return iso_str

    def update_kw_field(kw_id, field, value):
        try:
            supabase.table("keywords").update({field: value}).eq("id", kw_id).execute()
        except Exception as e:
            st.error(f"Помилка оновлення: {e}")

    # ========================================================
    # 2. БЛОК ДОДАВАННЯ
    # ========================================================
    with st.expander("➕ Додати нові запити", expanded=False): 
        with st.container(border=True):
            st.markdown("##### 📝 Введіть нові запити")
            for i in range(st.session_state["kw_input_count"]):
                st.text_input(f"Запит #{i+1}", key=f"new_kw_input_{i}", placeholder="Наприклад: Купити квитки...")

            col_plus, col_minus, _ = st.columns([1, 1, 5])
            with col_plus:
                if st.button("➕ Ще рядок"):
                    st.session_state["kw_input_count"] += 1
                    st.rerun()
            with col_minus:
                if st.session_state["kw_input_count"] > 1:
                    if st.button("➖ Прибрати"):
                        st.session_state["kw_input_count"] -= 1
                        st.rerun()

            st.divider()
            c_models, c_submit = st.columns([3, 1])
            with c_models:
                selected_models_add = st.multiselect("LLM для першого скану:", list(MODEL_MAPPING.keys()), default=["Perplexity"], key="add_multiselect")
            
            with c_submit:
                st.write("")
                st.write("")
                if st.button("🚀 Додати", use_container_width=True, type="primary"):
                    new_keywords_list = []
                    for i in range(st.session_state["kw_input_count"]):
                        val = st.session_state.get(f"new_kw_input_{i}", "").strip()
                        if val: new_keywords_list.append(val)
                    
                    if new_keywords_list:
                        try:
                            insert_data = [{
                                "project_id": proj["id"], "keyword_text": kw, "is_active": True, 
                                "is_auto_scan": False, "frequency": "daily"
                            } for kw in new_keywords_list]
                            
                            res = supabase.table("keywords").insert(insert_data).execute()
                            if res.data:
                                with st.spinner(f"Запускаємо аналіз..."):
                                    if 'n8n_trigger_analysis' in globals():
                                        for new_kw in new_keywords_list:
                                            n8n_trigger_analysis(proj["id"], [new_kw], proj.get("brand_name"), models=selected_models_add)
                                            time.sleep(0.5) 
                                st.success(f"Додано {len(new_keywords_list)} запитів!")
                                st.session_state["kw_input_count"] = 1
                                for key in list(st.session_state.keys()):
                                    if key.startswith("new_kw_input_"): del st.session_state[key]
                                time.sleep(1)
                                st.rerun()
                        except Exception as e:
                            st.error(f"Помилка: {e}")
                    else:
                        st.warning("Введіть хоча б один запит.")

    st.divider()
    
    # ========================================================
    # 3. ОТРИМАННЯ ДАНИХ
    # ========================================================
    try:
        keywords = supabase.table("keywords").select("*").eq("project_id", proj["id"]).order("created_at", desc=True).execute().data
        last_scans_resp = supabase.table("scan_results").select("keyword_id, created_at").eq("project_id", proj["id"]).order("created_at", desc=True).execute()
        
        last_scan_map = {}
        if last_scans_resp.data:
            for s in last_scans_resp.data:
                if s['keyword_id'] not in last_scan_map:
                    last_scan_map[s['keyword_id']] = s['created_at']
        
        for k in keywords:
            k['last_scan_date'] = last_scan_map.get(k['id'], "1970-01-01T00:00:00+00:00")

    except Exception as e:
        st.error(f"Помилка завантаження: {e}")
        keywords = []

    if not keywords:
        st.info("Список порожній.")
        return

    # ========================================================
    # 4. ПАНЕЛЬ УПРАВЛІННЯ
    # ========================================================
    c_sort, _ = st.columns([2, 4])
    with c_sort:
        sort_option = st.selectbox(
            "Сортувати за:", 
            ["Найновіші (Додані)", "Найстаріші (Додані)", "Нещодавно проскановані", "Давно не скановані"],
            label_visibility="collapsed"
        )

    if sort_option == "Найновіші (Додані)":
        keywords.sort(key=lambda x: x['created_at'], reverse=True)
    elif sort_option == "Найстаріші (Додані)":
        keywords.sort(key=lambda x: x['created_at'], reverse=False)
    elif sort_option == "Нещодавно проскановані":
        keywords.sort(key=lambda x: x['last_scan_date'], reverse=True)
    elif sort_option == "Давно не скановані":
        keywords.sort(key=lambda x: x['last_scan_date'], reverse=False)

    with st.container(border=True):
        c_check, c_models, c_btn = st.columns([0.5, 3, 1.5])
        with c_check:
            st.write("") 
            select_all = st.checkbox("Всі", key="select_all_kws")
        with c_models:
            bulk_models = st.multiselect("ЛЛМ для запуску:", list(MODEL_MAPPING.keys()), default=["Perplexity"], label_visibility="collapsed", key="bulk_models_main")
        with c_btn:
            # 🔥 PRIMARY BUTTON (Яскрава)
            if st.button("🚀 Аналізувати обрані", use_container_width=True, type="primary"):
                selected_kws_text = []
                if select_all:
                    selected_kws_text = [k['keyword_text'] for k in keywords]
                else:
                    for k in keywords:
                        if st.session_state.get(f"chk_{k['id']}", False):
                            selected_kws_text.append(k['keyword_text'])
                
                if selected_kws_text:
                    my_bar = st.progress(0, text="Ініціалізація...")
                    total_kws = len(selected_kws_text)
                    try:
                        if 'n8n_trigger_analysis' in globals():
                            for i, single_kw in enumerate(selected_kws_text):
                                my_bar.progress((i / total_kws), text=f"Відправка: {single_kw}...")
                                n8n_trigger_analysis(proj["id"], [single_kw], proj.get("brand_name"), models=bulk_models)
                                time.sleep(0.3)
                            my_bar.progress(1.0, text="Готово!")
                            st.success(f"Успішно запущено {total_kws} завдань! Оновіть сторінку за хвилину.")
                            time.sleep(2)
                            st.rerun()
                        else:
                            st.error("Функція запуску не знайдена.")
                    except Exception as e:
                        st.error(f"Помилка при запуску: {e}")
                else:
                    st.warning("Оберіть хоча б один запит.")

    # ========================================================
    # 5. СПИСОК ЗАПИТІВ (ОНОВЛЕНИЙ)
    # ========================================================
    
    h_chk, h_num, h_txt, h_cron, h_date, h_act = st.columns([0.4, 0.5, 3.2, 2, 1.2, 1.3])
    h_txt.markdown("**Запит**")
    h_cron.markdown("**Автозапуск**")
    h_date.markdown("**Останній аналіз**")
    h_act.markdown("**Видалити**")

    for idx, k in enumerate(keywords, start=1):
        with st.container(border=True):
            c1, c2, c3, c4, c5, c6 = st.columns([0.4, 0.5, 3.2, 2, 1.2, 1.3])
            
            with c1:
                st.write("") 
                is_checked = select_all
                st.checkbox("", key=f"chk_{k['id']}", value=is_checked)
            
            with c2:
                st.markdown(f"<div class='green-number'>{idx}</div>", unsafe_allow_html=True)
            
            with c3:
                # Кнопка без фону (через CSS)
                if st.button(k['keyword_text'], key=f"link_btn_{k['id']}", help="Натисніть для детального аналізу"):
                    st.session_state["focus_keyword_id"] = k["id"]
                    st.rerun()
            
            with c4:
                cron_c1, cron_c2 = st.columns([0.8, 1.2])
                is_auto = k.get('is_auto_scan', False) 
                
                with cron_c1:
                    new_auto = st.toggle("Авто", value=is_auto, key=f"auto_{k['id']}", label_visibility="collapsed")
                    if new_auto != is_auto:
                        update_kw_field(k['id'], "is_auto_scan", new_auto)
                        st.rerun()

                with cron_c2:
                    if new_auto:
                        current_freq = k.get('frequency', 'daily')
                        freq_options = ["daily", "weekly", "monthly"]
                        try: idx_f = freq_options.index(current_freq)
                        except: idx_f = 0
                        new_freq = st.selectbox("Freq", freq_options, index=idx_f, key=f"freq_{k['id']}", label_visibility="collapsed")
                        if new_freq != current_freq:
                            update_kw_field(k['id'], "frequency", new_freq)
                    else:
                        st.caption("Вимкнено")
            
            with c5:
                st.write("")
                date_iso = k.get('last_scan_date')
                formatted_date = format_kyiv_time(date_iso)
                st.caption(f"{formatted_date}")
            
            with c6:
                st.write("")
                
                del_key = f"confirm_del_kw_{k['id']}"
                if del_key not in st.session_state: st.session_state[del_key] = False

                if not st.session_state[del_key]:
                    # Стандартна кнопка видалення
                    if st.button("🗑️ Видалити", key=f"pre_del_{k['id']}"):
                        st.session_state[del_key] = True
                        st.rerun()
                else:
                    dc1, dc2 = st.columns(2)
                    if dc1.button("✅", key=f"yes_del_{k['id']}", type="primary"):
                        try:
                            supabase.table("scan_results").delete().eq("keyword_id", k["id"]).execute()
                            supabase.table("keywords").delete().eq("id", k["id"]).execute()
                            st.success("!")
                            st.session_state[del_key] = False
                            time.sleep(0.5)
                            st.rerun()
                        except Exception as e:
                            st.error("Помилка")
                    
                    if dc2.button("❌", key=f"no_del_{k['id']}"):
                        st.session_state[del_key] = False
                        st.rerun()


# =========================
# 8. РЕКОМЕНДАЦІЇ
# =========================


def show_recommendations_page():
    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект.")
        return

    st.title("💡 Центр Стратегій та Дій")
    st.caption("ШІ-аналітик аналізує ваші позиції та генерує покроковий план дій.")

    # 1. Розділяємо на типи рекомендацій
    # Ми використовуємо константи, які співпадають з ENUM в базі даних (rec_type)
    tabs = st.tabs(["📣 PR Стратегія", "💻 Digital & SEO", "✍️ Контент-план", "📱 Social Media"])
    types = ["pr", "digital", "content", "social"]

    for tab, r_type in zip(tabs, types):
        with tab:
            # --- Блок Генерації (Тільки для Адміна або якщо дозволено юзеру) ---
            # Можна додати перевірку: if st.session_state["role"] == "admin":
            
            with st.container(border=True):
                c1, c2 = st.columns([3, 1])
                with c1:
                    st.markdown(f"**Згенерувати новий звіт: {r_type.upper()}**")
                    st.caption("Аналіз останніх 30 днів, пошук розривів (gaps) та план дій.")
                with c2:
                    if st.button(f"🚀 Запустити AI", key=f"btn_gen_{r_type}"):
                        with st.spinner("Gemini аналізує дані та пише звіт... Це може зайняти до 30 секунд."):
                            try:
                                # Тут ми викликаємо n8n вебхук
                                # Для MVP поки що робимо запис-заглушку, якщо n8n не підключений
                                # АБО викликаємо реальну функцію n8n_request_recommendations
                                
                                # Варіант А: Реальний виклик (розкоментуйте, коли буде готовий n8n)
                                # n8n_request_recommendations(proj, r_type, "Auto-generated report")
                                
                                # Варіант Б: Симуляція (щоб ви побачили як це виглядає зараз)
                                fake_report = f"""
                                # Стратегія {r_type.upper()} для {proj.get('brand_name')}
                                **Дата:** {datetime.now().strftime('%Y-%m-%d')}
                                
                                ## 1. Аналіз ситуації
                                Наразі частка голосу (SOV) складає **{proj.get('sov', '15')}%**.
                                Основні конкуренти домінують у категорії "Депозити".
                                
                                ## 2. Ключові проблеми
                                * Відсутність згадок на *Minfin.com.ua*.
                                * Низька тональність у відповідях Perplexity.
                                
                                ## 3. План дій (Action Items)
                                1. **Стаття-огляд:** Замовити розміщення на finance.ua.
                                2. **Робота з відгуками:** Відповісти на скарги на форумах.
                                """
                                
                                supabase.table("recommendation_reports").insert({
                                    "project_id": proj["id"],
                                    "report_type": r_type,
                                    "report_content": fake_report
                                }).execute()
                                
                                st.success("Звіт успішно згенеровано!")
                                time.sleep(1)
                                st.rerun()
                                
                            except Exception as e:
                                st.error(f"Помилка генерації: {e}")

            st.divider()
            st.subheader("📂 Історія звітів")

            # 2. Виведення історії звітів з бази
            try:
                reports = (
                    supabase.table("recommendation_reports")
                    .select("*")
                    .eq("project_id", proj["id"])
                    .eq("report_type", r_type)
                    .order("created_at", desc=True)
                    .execute()
                    .data
                )
            except Exception as e:
                st.error(f"Помилка завантаження: {e}")
                reports = []

            if not reports:
                st.info("У цій категорії ще немає звітів.")
            else:
                for rep in reports:
                    date_str = str(rep['created_at'])[:10]
                    with st.expander(f"📄 Звіт від {date_str}"):
                        # Кнопка видалення звіту
                        if st.button("Видалити звіт", key=f"del_rep_{rep['id']}"):
                            supabase.table("recommendation_reports").delete().eq("id", rep['id']).execute()
                            st.rerun()
                        
                        st.markdown("---")
                        # Рендеринг Markdown (основна фішка)
                        st.markdown(rep['report_content'])

# =========================
# 9. SIDEBAR
# =========================
def show_sources_page():
    """
    Сторінка джерел.
    ВЕРСІЯ: TABLE EDITOR + TAGS + LIVE COUNTS.
    1. UI: Замість expander використовується st.data_editor.
    2. Features: Додано вибір тегів (Web, Social, Author).
    3. Metrics: Навпроти кожного домену показується кількість його згадок у базі.
    4. Data: Зберігається як JSONB у projects.official_assets.
    """
    import pandas as pd
    import plotly.express as px
    import streamlit as st
    import time
    from urllib.parse import urlparse

    if 'supabase' not in globals():
        if 'supabase' in st.session_state:
            supabase = st.session_state['supabase']
        else:
            st.error("🚨 Помилка: Змінна 'supabase' не знайдена.")
            return
    else:
        supabase = globals()['supabase']

    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку оберіть проект.")
        return

    st.title("🔗 Джерела та Охоплення")

    # ==============================================================================
    # 1. ЗАВАНТАЖЕННЯ ДАНИХ
    # ==============================================================================
    
    # А. Офіційні ресурси (Whitelist)
    try:
        project_data = supabase.table("projects").select("official_assets").eq("id", proj["id"]).execute()
        raw_assets = project_data.data[0].get("official_assets", []) if project_data.data else []
        
        # Нормалізація даних (якщо це старий список рядків -> конвертуємо в об'єкти)
        assets_list = []
        if isinstance(raw_assets, list):
            for item in raw_assets:
                if isinstance(item, str):
                    assets_list.append({"domain": item, "type": "Вебсайт"})
                elif isinstance(item, dict):
                    assets_list.append(item)
        else:
            assets_list = []
            
    except Exception as e:
        st.error(f"Помилка завантаження налаштувань (спробуйте виконати SQL-фікс): {e}")
        assets_list = []

    # Б. Всі знайдені джерела (для підрахунку статистики)
    try:
        scan_ids_resp = supabase.table("scan_results").select("id").eq("project_id", proj["id"]).execute()
        scan_ids = [s['id'] for s in scan_ids_resp.data] if scan_ids_resp.data else []
        
        df_all_sources = pd.DataFrame()
        if scan_ids:
            # Обмежуємо вибірку, якщо даних дуже багато, але для статистики треба все
            src_resp = supabase.table("extracted_sources").select("url").in_("scan_result_id", scan_ids).execute()
            if src_resp.data:
                df_all_sources = pd.DataFrame(src_resp.data)
                # Витягуємо чистий домен для порівняння
                df_all_sources['clean_domain'] = df_all_sources['url'].apply(lambda x: urlparse(x).netloc.lower() if x else "")
    except Exception as e:
        st.warning(f"Не вдалося завантажити статистику згадок: {e}")
        df_all_sources = pd.DataFrame()

    # ==============================================================================
    # 2. ПІДРАХУНОК ЗГАДОК (LIVE COUNT)
    # ==============================================================================
    
    def count_mentions(domain_to_check):
        if df_all_sources.empty or not domain_to_check:
            return 0
        domain_to_check = domain_to_check.lower().strip()
        # Рахуємо входження підрядка (наприклад, skyup.aero в www.skyup.aero)
        mask = df_all_sources['clean_domain'].str.contains(domain_to_check, regex=False)
        return mask.sum()

    # Створюємо DataFrame для редактора
    if assets_list:
        df_editor = pd.DataFrame(assets_list)
    else:
        df_editor = pd.DataFrame(columns=["domain", "type"])

    # Додаємо колонку згадок (якщо її немає)
    if "domain" in df_editor.columns:
        df_editor["mentions"] = df_editor["domain"].apply(count_mentions)
    else:
        df_editor["mentions"] = 0

    # ==============================================================================
    # 3. РЕДАКТОР ОФІЦІЙНИХ РЕСУРСІВ (БЕЗ АКОРДЕОНУ)
    # ==============================================================================
    
    st.markdown("### ⚙️ Керування офіційними ресурсами")
    st.caption("Додайте домени, які належать вашому бренду. Система автоматично підрахує кількість їх згадок у відповідях LLM.")

    edited_df = st.data_editor(
        df_editor,
        num_rows="dynamic",
        use_container_width=True,
        column_config={
            "domain": st.column_config.TextColumn(
                "Домен / URL",
                help="Наприклад: skyup.aero або instagram.com/skyup",
                placeholder="site.com",
                validate="^.+$",
                required=True
            ),
            "type": st.column_config.SelectboxColumn(
                "Тип ресурсу",
                help="Категорія для аналітики",
                width="medium",
                options=[
                    "Вебсайт",
                    "Соціальні мережі",
                    "Автор / Блог",
                    "Партнер",
                    "Інше"
                ],
                required=True,
                default="Вебсайт"
            ),
            "mentions": st.column_config.NumberColumn(
                "Знайдено згадок",
                help="Скільки разів цей домен зустрічався у скануваннях (автоматично)",
                disabled=True, # Забороняємо редагувати статистику
                format="%d 👁️"
            )
        },
        key="sources_editor"
    )

    # Кнопка збереження
    if st.button("💾 Зберегти зміни", type="primary"):
        try:
            # Конвертуємо назад у JSON, ігноруючи колонку mentions (вона динамічна)
            records_to_save = []
            for _, row in edited_df.iterrows():
                d_val = str(row.get("domain", "")).strip()
                t_val = str(row.get("type", "Вебсайт"))
                if d_val:
                    records_to_save.append({"domain": d_val, "type": t_val})
            
            # Оновлюємо базу
            supabase.table("projects").update({"official_assets": records_to_save}).eq("id", proj["id"]).execute()
            
            st.success("Список оновлено! Статистика перераховується...")
            time.sleep(1)
            st.rerun()
        except Exception as e:
            st.error(f"Помилка збереження. Переконайтеся, що ви виконали SQL-запит для оновлення структури бази. Деталі: {e}")

    # Список доменів для фільтрації графіків знизу
    OFFICIAL_DOMAINS = [r['domain'].lower() for r in records_to_save] if 'records_to_save' in locals() else [str(r.get('domain','')).lower() for r in assets_list]

    st.divider()

    # ==============================================================================
    # 4. ВІЗУАЛІЗАЦІЯ (ГРАФІКИ)
    # ==============================================================================
    
    # Підготовка даних для графіків
    # Використовуємо df_all_sources, який ми вже завантажили вище
    if not df_all_sources.empty:
        # Функція перевірки офіційності (динамічна)
        def check_is_official(clean_url):
            for od in OFFICIAL_DOMAINS:
                if od in clean_url: return True
            return False
        
        df_all_sources['is_official_calc'] = df_all_sources['clean_domain'].apply(check_is_official)
        
        # Статистика
        total_links = len(df_all_sources)
        official_links = df_all_sources['is_official_calc'].sum()
        external_links = total_links - official_links
    else:
        total_links, official_links, external_links = 0, 0, 0

    tab_g1, tab_g2 = st.tabs(["📊 Охоплення", "🔗 Всі посилання"])

    with tab_g1:
        c_chart, c_stat = st.columns([2, 1])
        with c_chart:
            if total_links > 0:
                plot_data = pd.DataFrame({
                    "Тип": ["Офіційні", "Зовнішні"],
                    "Кількість": [official_links, external_links]
                })
                fig = px.pie(
                    plot_data, values="Кількість", names="Тип", hole=0.6,
                    color="Тип", color_discrete_map={"Офіційні": "#00C896", "Зовнішні": "#E0E0E0"}
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Немає даних для графіка.")
        
        with c_stat:
            st.markdown("#### Загальна статистика")
            st.metric("Всього посилань", total_links)
            st.metric("Офіційних ресурсів", official_links, delta=f"{official_links/total_links*100:.1f}%" if total_links else None)

    with tab_g2:
        if not df_all_sources.empty:
            st.dataframe(
                df_all_sources, 
                use_container_width=True,
                column_config={
                    "url": st.column_config.LinkColumn("Посилання"),
                    "clean_domain": "Домен",
                    "is_official_calc": "Офіційний?"
                }
            )
        else:
            st.info("Список посилань порожній.")


def sidebar_menu():
    from streamlit_option_menu import option_menu
    
    # Отримуємо дані
    user = st.session_state.get("user")
    role = st.session_state.get("role", "user")
    current_proj = st.session_state.get("current_project")

    # --- 🎨 CSS ДЛЯ АДМІНА (Заливка сайдбару) ---
    if role == "admin":
        st.markdown("""
        <style>
            [data-testid="stSidebar"] {
                background-color: #E8F5E9; /* Світло-зелений фон для Адміна */
                border-right: 2px solid #00C896; /* Акцентна лінія справа */
            }
            /* Можна підфарбувати заголовки, щоб було стильно */
            [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3 {
                color: #00695C;
            }
        </style>
        """, unsafe_allow_html=True)

    with st.sidebar:
        # 1. ЛОГОТИП
        st.image("https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png", width=150) 
        #st.markdown("## AI Visibility by Virshi") 

        # Профіль
        user_name = "Користувач"
        if user:
            meta = user.user_metadata
            user_name = meta.get("full_name") or meta.get("name") or user.email.split("@")[0]

        st.caption(f"👤 {user_name}")
        
        # ❌ ТУТ ПРИБРАЛИ НАПИС "Admin Mode"
        
        st.divider()

        # 2. ВИБІР ПРОЕКТУ
        if role == "admin":
            try:
                if 'supabase' in globals():
                    projs_resp = supabase.table("projects").select("id, brand_name, status").execute()
                    projects_list = projs_resp.data
                else:
                    projects_list = []

                options_map = {f"{p['brand_name']} (ID: {p['id']})": p for p in projects_list}
                
                current_index = 0
                if current_proj:
                    current_key = f"{current_proj['brand_name']} (ID: {current_proj['id']})"
                    if current_key in options_map:
                        current_index = list(options_map.keys()).index(current_key)

                selected_key = st.selectbox(
                    "📂 Оберіть проект:",
                    options=list(options_map.keys()),
                    index=current_index,
                    placeholder="Пошук по Назві або ID...",
                    help="Введіть ID для пошуку"
                )

                if selected_key:
                    new_proj = options_map[selected_key]
                    if not current_proj or new_proj['id'] != current_proj['id']:
                        st.session_state["current_project"] = new_proj
                        st.rerun()

            except Exception as e:
                st.error(f"Error: {e}")

        else:
            # ЮЗЕР
            if current_proj:
                st.markdown(f"### 📂 {current_proj.get('brand_name')}")
                with st.expander("ℹ️ Project ID"):
                    st.code(current_proj.get('id'), language=None)
            else:
                st.warning("Проект не обрано")

        st.write("") 

    # 3. НАВІГАЦІЯ
    with st.sidebar:
        selected = option_menu(
            "Меню",
            ["Дашборд", "Перелік запитів", "Джерела", "Конкуренти", "Рекомендації", "GPT-Visibility", "Адмін"] if role == "admin" else ["Дашборд", "Перелік запитів", "Джерела", "Конкуренти", "Рекомендації", "GPT-Visibility"],
            icons=["speedometer2", "list-task", "router", "people", "lightbulb", "robot", "shield-lock"],
            menu_icon="cast",
            default_index=0,
            styles={
                "nav-link-selected": {"background-color": "#00C896"}, 
            }
        )

    # 4. ФУТЕР
    with st.sidebar:
        st.divider()
        
        # Статус плану
        if st.session_state.get("current_project"):
            status_text = st.session_state["current_project"].get("status", "TRIAL").upper()
            color = "#FFA500" if "TRIAL" in status_text else "#00C896"
            st.markdown(f"Статус: <span style='color:{color}; font-weight:bold;'>● {status_text}</span>", unsafe_allow_html=True)
        
        st.write("")
        
        # ✅ ТУТ ДОДАЛИ НАПИС "Admin Mode" (Тільки для адміна)
        if role == "admin":
            st.caption("🛡️ Admin Mode")

        # Support
        st.caption("Support: hi@virshi.ai")

        # Кнопка Виходу
        if st.button("Вийти з акаунту", key="logout_btn", use_container_width=True):
            logout()

    return selected

def show_auth_page():
    """
    Renders the centered authentication card (Login / Register) with Virshi styling.
    """
    # Apply custom CSS for the auth page
    st.markdown("""
    <style>
        /* General Page Background */
        .stApp {
            background-color: #F4F7F6;
        }
        
        /* Center the form container */
        [data-testid="stForm"] {
            background-color: #ffffff;
            padding: 40px;
            border-radius: 12px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.08);
            border: 1px solid #EAEAEA;
        }

        /* Input fields styling */
        .stTextInput > div > div > input {
            border-radius: 8px;
            border: 1px solid #e0e0e0;
            padding: 10px;
        }

        /* Primary Button (Virshi Green) */
        .stButton > button {
            width: 100%;
            background-color: #00C896 !important;
            color: white !important;
            border: none;
            border-radius: 8px;
            padding: 12px;
            font-weight: 600;
            margin-top: 10px;
        }
        .stButton > button:hover {
            background-color: #00a87e !important;
        }
        
        /* Tabs Styling */
        .stTabs [data-baseweb="tab-list"] {
            gap: 20px;
            justify-content: center;
        }
        .stTabs [data-baseweb="tab"] {
            height: 50px;
            white-space: pre-wrap;
            border-radius: 4px 4px 0 0;
            gap: 1px;
            padding-top: 10px;
            padding-bottom: 10px;
        }
    </style>
    """, unsafe_allow_html=True)

    # Centering Layout using Columns
    # [Empty Left] [Center Card] [Empty Right]
    col_l, col_center, col_r = st.columns([1, 1.5, 1])

    with col_center:
        # Logo Section
        st.markdown(
            '<div style="text-align: center; margin-bottom: 20px;">'
            '<img src="https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png" width="180">'
            '</div>',
            unsafe_allow_html=True,
        )
        
        st.markdown("<h3 style='text-align: center; color: #333; margin-bottom: 5px;'>Welcome to Virshi</h3>", unsafe_allow_html=True)
        st.markdown("<p style='text-align: center; color: #666; margin-bottom: 30px;'>Sign in to manage your AI visibility</p>", unsafe_allow_html=True)

        # Tabs for Login / Register
        tab_login, tab_register = st.tabs(["🔑 Sign In", "📝 Sign Up"])

        # --- LOGIN TAB ---
        with tab_login:
            with st.form("login_form"):
                email = st.text_input("Email", placeholder="name@company.com")
                password = st.text_input("Password", type="password", placeholder="••••••••")
                
                st.write("") # Spacer
                
                submit = st.form_submit_button("Sign In", use_container_width=True)
                
                if submit:
                    if not email or not password:
                        st.warning("Please fill in all fields.")
                    else:
                        login_user(email, password)

        # --- REGISTER TAB ---
        with tab_register:
            with st.form("register_form"):
                c1, c2 = st.columns(2)
                with c1:
                    first_name = st.text_input("First Name", placeholder="Ivan")
                with c2:
                    last_name = st.text_input("Last Name", placeholder="Petrenko")
                
                new_email = st.text_input("Email", placeholder="name@company.com")
                new_password = st.text_input("Password", type="password", placeholder="••••••••", help="Min 6 chars")
                
                st.write("") # Spacer
                
                submit_reg = st.form_submit_button("Create Account", use_container_width=True)
                
                if submit_reg:
                    if not new_email or not new_password or not first_name:
                        st.warning("Please fill in required fields.")
                    elif len(new_password) < 6:
                        st.warning("Password must be at least 6 characters.")
                    else:
                        register_user(new_email, new_password, first_name, last_name)



def show_admin_page():
    """
    Адмін-панель (CRM).
    Версія 5.1 (Auth Added):
    - Webhook trigger_keyword_generation тепер має авторизацію.
    """
    import pandas as pd
    import streamlit as st
    import numpy as np
    import requests
    import json

    # --- КОНСТАНТИ ---
    N8N_GEN_URL = "https://virshi.app.n8n.cloud/webhook/webhook/generate-prompts"

    # --- 0. ПІДКЛЮЧЕННЯ ---
    if 'supabase' not in globals():
        if 'supabase' in st.session_state:
            supabase = st.session_state['supabase']
        else:
            st.error("🚨 Помилка: Змінна 'supabase' не знайдена.")
            return
    else:
        supabase = globals()['supabase']

    # --- ХЕЛПЕРИ ---
    def clean_data_for_json(data):
        if isinstance(data, dict): return {k: clean_data_for_json(v) for k, v in data.items()}
        elif isinstance(data, list): return [clean_data_for_json(v) for v in data]
        elif isinstance(data, (np.int64, np.int32, np.integer)): return int(data)
        elif isinstance(data, (np.float64, np.float32, np.floating)): return float(data)
        elif isinstance(data, (np.bool_, bool)): return bool(data)
        elif pd.isna(data): return None
        return data

    def update_project_field(proj_id, field, value):
        try:
            val = clean_data_for_json(value)
            supabase.table("projects").update({field: val}).eq("id", proj_id).execute()
        except Exception as e:
            st.error(f"Помилка оновлення: {e}")

    # --- ЛОГІКА ВЕБХУКА З АВТОРИЗАЦІЄЮ ---
    def trigger_keyword_generation(brand, domain, industry, products):
        """Відправляє повний набір даних на n8n з Auth"""
        payload = {
            "brand": brand,
            "domain": domain,
            "industry": industry,
            "products": products
        }
        
        # 🔥 HEADER AUTH
        headers = {
            "virshi-auth": "hi@virshi.ai2025"
        }
        
        try:
            # Додано headers=headers
            response = requests.post(
                N8N_GEN_URL, 
                json=payload, 
                headers=headers, 
                timeout=25
            )
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    # Обробка різних варіантів відповіді n8n
                    if isinstance(data, dict):
                        if "prompts" in data: return data["prompts"]
                        if "keywords" in data: return data["keywords"]
                        return list(data.values()) if data else []
                    elif isinstance(data, list):
                        return data
                    else:
                        st.warning(f"Нестандартна відповідь: {data}")
                        return []
                except ValueError:
                    st.error("N8N повернув не JSON.")
                    return []
            else:
                st.error(f"Помилка вебхука: {response.status_code}")
                return []
        except Exception as e:
            st.error(f"Помилка з'єднання: {e}")
            return []

    # Ініціалізація стану для нових запитів
    if "new_proj_keywords" not in st.session_state:
        st.session_state["new_proj_keywords"] = []

    st.title("🛡️ Admin Panel (CRM)")

    # --- 1. ОТРИМАННЯ ДАНИХ ---
    try:
        projects_resp = supabase.table("projects").select("*").order("created_at", desc=True).execute()
        projects_data = projects_resp.data if projects_resp.data else []

        users_resp = supabase.table("profiles").select("*").execute()
        users_data = users_resp.data if users_resp.data else []
        
        user_map = {}
        for u in users_data:
            f_name = u.get('first_name', '') or ''
            l_name = u.get('last_name', '') or ''
            full_name = f"{f_name} {l_name}".strip() or u.get('email', 'Unknown')
            user_map[u['id']] = {
                "full_name": full_name,
                "role": u.get('role', 'user'),
                "email": u.get('email', '-')
            }

    except Exception as e:
        st.error(f"Помилка завантаження даних: {e}")
        return

    # --- 2. KPI ---
    if projects_data:
        df_stats = pd.DataFrame(projects_data)
        total = len(df_stats)
        active = len(df_stats[df_stats['status'] == 'active'])
        blocked = len(df_stats[df_stats['status'] == 'blocked'])
        trial = len(df_stats[df_stats['status'] == 'trial'])
        
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Всього", total)
        k2.metric("Active (Paid)", active)
        k3.metric("Trial", trial)
        k4.metric("Blocked", blocked)

    st.write("")

    # --- 3. ВКЛАДКИ ---
    tab_list, tab_create, tab_users = st.tabs(["📂 Список проектів", "➕ Створити проект", "👥 Користувачі & Права"])

    # ========================================================
    # TAB 1: СПИСОК ПРОЕКТІВ
    # ========================================================
    with tab_list:
        st.markdown("##### Керування проектами")
        
        h0, h1, h_dash, h2, h3, h4, h5 = st.columns([0.3, 2, 0.5, 1.5, 1.5, 1, 0.5])
        h0.markdown("**#**")
        h1.markdown("**Проект / Користувач**")
        h_dash.markdown("") 
        h2.markdown("**Статус**")
        h3.markdown("**Авто сканування**")
        h4.markdown("**Дата**")
        h5.markdown("**Дії**")
        st.divider()

        if not projects_data:
            st.info("Проектів немає.")

        for idx, p in enumerate(projects_data, 1):
            p_id = p['id']
            u_id = p.get('user_id')
            owner_info = user_map.get(u_id, {"full_name": "Невідомий", "role": "user", "email": "-"})
            
            raw_name = p.get('project_name')
            domain = p.get('domain', '')
            if not raw_name or raw_name.strip() == "" or raw_name == "No Name":
                p_name = domain.split('.')[0].capitalize() if domain else "Без назви"
            else:
                p_name = raw_name

            with st.container():
                c0, c1, c_dash, c2, c3, c4, c5 = st.columns([0.3, 2, 0.5, 1.5, 1.5, 1, 0.5])

                with c0: st.caption(f"{idx}")

                with c1:
                    st.markdown(f"**{p_name}**")
                    st.caption(f"ID: `{p_id}`")
                    st.caption(f"🌐 {domain}")
                    st.caption(f"👤 {owner_info['full_name']} ({owner_info['role']})")

                with c_dash:
                    if st.button("↗️", key=f"goto_{p_id}", help=f"Перейти до дашборду '{p_name}'"):
                        st.session_state["current_project"] = p
                        st.session_state["focus_keyword_id"] = None
                        if "selected_page" in st.session_state:
                            st.session_state["selected_page"] = "Дашборд"
                        st.rerun()

                with c2:
                    curr_status = p.get('status', 'trial')
                    opts = ["trial", "active", "blocked"]
                    try: idx_s = opts.index(curr_status)
                    except: idx_s = 0
                    
                    new_status = st.selectbox("St", opts, index=idx_s, key=f"st_{p_id}", label_visibility="collapsed")
                    if new_status != curr_status:
                        update_project_field(p_id, "status", new_status)
                        st.rerun()

                with c3:
                    allow_cron = p.get('allow_cron', False)
                    new_cron = st.checkbox("Дозволити", value=allow_cron, key=f"cr_{p_id}")
                    if new_cron != allow_cron:
                        update_project_field(p_id, "allow_cron", new_cron)
                        st.rerun()

                with c4:
                    raw_date = p.get('created_at', '')
                    if raw_date: st.caption(raw_date[:10])

                with c5:
                    confirm_key = f"confirm_del_{p_id}"
                    if not st.session_state.get(confirm_key, False):
                        if st.button("🗑", key=f"del_btn_{p_id}"):
                            st.session_state[confirm_key] = True
                            st.rerun()
                    else:
                        if st.button("✅", key=f"yes_{p_id}"):
                            if owner_info['role'] == 'super_admin':
                                st.error("Super Admin!")
                                st.session_state[confirm_key] = False
                            else:
                                try:
                                    supabase.table("projects").delete().eq("id", p_id).execute()
                                    if u_id: supabase.table("profiles").delete().eq("id", u_id).execute()
                                    st.success("Видалено!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(str(e))
                        if st.button("❌", key=f"no_{p_id}"):
                            st.session_state[confirm_key] = False
                            st.rerun()
                st.divider()

    # ========================================================
    # TAB 2: СТВОРИТИ ПРОЕКТ (REAL WEBHOOK)
    # ========================================================
    with tab_create:
        st.markdown("##### Створення нового проекту")
        
        c1, c2 = st.columns(2)
        new_name_val = c1.text_input("Назва проекту (Бренд)", key="new_proj_name", placeholder="Наприклад: SkyUp")
        new_domain_val = c2.text_input("Домен", key="new_proj_domain", placeholder="skyup.aero")
        
        c3, c4 = st.columns(2)
        new_industry_val = c3.text_input("Галузь (Обов'язково)", key="new_proj_ind", placeholder="напр. авіаперевезення")
        new_desc_val = c4.text_area("Продукти/Послуги", placeholder="напр. лоукостер, квитки", height=68, key="new_proj_desc")
        
        if st.button("✨ Згенерувати 10 запитів (AI)"):
            if new_domain_val and new_industry_val and new_desc_val: 
                brand_for_ai = new_name_val if new_name_val else new_domain_val.split('.')[0]
                
                with st.spinner("Звертаємось до n8n для генерації..."):
                    generated_kws = trigger_keyword_generation(
                        brand=brand_for_ai,
                        domain=new_domain_val,
                        industry=new_industry_val,
                        products=new_desc_val
                    )
                
                if generated_kws:
                    st.session_state["new_proj_keywords"] = [{"keyword": kw} for kw in generated_kws]
                    st.success(f"Успішно згенеровано {len(generated_kws)} запитів!")
                else:
                    st.warning("Вебхук не повернув даних. Перевірте логи.")
            else:
                st.warning("⚠️ Заповніть: Домен, Галузь та Продукти.")

        st.divider()
        st.markdown("###### 📝 Редагування запитів перед створенням")
        st.caption("Ви можете редагувати текст, видаляти рядки (Del) та додавати нові (кнопка + знизу).")

        df_initial = pd.DataFrame(st.session_state["new_proj_keywords"])
        if df_initial.empty:
            df_initial = pd.DataFrame(columns=["keyword"])

        edited_df = st.data_editor(
            df_initial,
            num_rows="dynamic",
            column_config={
                "keyword": st.column_config.TextColumn(
                    "Список запитів",
                    width="large",
                    required=True,
                    help="Введіть запит тут"
                )
            },
            use_container_width=True,
            key="editor_new_kws",
            hide_index=False
        )

        st.write("")
        c_st, c_cr = st.columns(2)
        new_status = c_st.selectbox("Початковий статус", ["trial", "active", "blocked"], key="new_proj_status")
        new_cron = c_cr.checkbox("Дозволити автосканування одразу?", value=False, key="new_proj_cron")

        if st.button("🚀 Створити проект та зберегти запити", type="primary"):
            final_name = new_name_val if new_name_val else new_domain_val.split('.')[0].capitalize()
            
            if new_domain_val:
                try:
                    new_proj_data = {
                        "project_name": final_name,
                        "domain": new_domain_val,
                        "status": new_status,
                        "allow_cron": new_cron
                    }
                    res_proj = supabase.table("projects").insert(new_proj_data).execute()
                    
                    if res_proj.data:
                        new_proj_id = res_proj.data[0]['id']
                        
                        final_kws_list = edited_df["keyword"].dropna().tolist()
                        final_kws_list = [str(k).strip() for k in final_kws_list if str(k).strip()]
                        
                        if final_kws_list:
                            kws_data = [
                                {
                                    "project_id": new_proj_id, 
                                    "keyword_text": kw,
                                    "is_active": True
                                } for kw in final_kws_list
                            ]
                            supabase.table("keywords").insert(kws_data).execute()
                        
                        st.success(f"Проект '{final_name}' створено! Додано {len(final_kws_list)} запитів.")
                        st.session_state["new_proj_keywords"] = [] 
                        st.rerun()
                except Exception as e:
                    st.error(f"Помилка створення: {e}")
            else:
                st.warning("Домен обов'язковий.")

    # ========================================================
    # TAB 3: КОРИСТУВАЧІ ТА ПРАВА
    # ========================================================
    with tab_users:
        st.markdown("##### 👥 База користувачів")

        if users_data:
            df_users = pd.DataFrame(users_data)
            required_cols = ['id', 'email', 'first_name', 'last_name', 'role']
            for col in required_cols:
                if col not in df_users.columns: df_users[col] = None

            edited_users = st.data_editor(
                df_users[required_cols],
                column_config={
                    "id": st.column_config.TextColumn("User ID", disabled=True, width="small"),
                    "email": st.column_config.TextColumn("Email", disabled=True),
                    "first_name": "Ім'я",
                    "last_name": "Прізвище",
                    "role": st.column_config.SelectboxColumn("Роль", options=["user", "admin", "super_admin"], required=True)
                },
                hide_index=True,
                use_container_width=True,
                key="admin_users_editor_v3"
            )

            if st.button("💾 Зберегти зміни прав"):
                try:
                    updated_rows = edited_users.to_dict('records')
                    for row in updated_rows:
                        clean_row = clean_data_for_json(row)
                        uid = clean_row.pop('id')
                        if 'email' in clean_row: del clean_row['email']
                        supabase.table("profiles").update(clean_row).eq("id", uid).execute()
                    st.success("Оновлено!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Помилка: {e}")
        else:
            st.warning("Користувачів не знайдено.")

def show_chat_page():
    """
    Сторінка AI-асистента (GPT-Visibility).
    Виправлено: 
    - Змінено назву заголовка авторизації на валідну (без спецсимволів).
    - Передача контексту (user_id, project_id, role).
    """
    import requests
    import streamlit as st

    # --- КОНФІГУРАЦІЯ ---    
    # 🔥 ВИПРАВЛЕНА АВТОРИЗАЦІЯ
    # Назва заголовка не повинна містити '@'. 
    # Змініть 'Name' в n8n Credentials на 'virshi-auth'
    headers = {
        "virshi-auth": "hi@virshi.ai2025" 
    }

    st.title("🤖 GPT-Visibility Assistant")
    
    # 1. Отримуємо контекст
    user = st.session_state.get("user")
    role = st.session_state.get("role", "user") 
    proj = st.session_state.get("current_project", {})
    
    if not proj:
        st.warning("⚠️ Будь ласка, оберіть проект у меню зліва.")

    # 2. Ініціалізація локальної історії
    if "messages" not in st.session_state:
        brand_name = proj.get('brand_name', 'вашого бренду') if proj else 'вашого бренду'
        welcome_text = f"Привіт! Я аналітик проекту **{brand_name}**. Готовий допомогти."
        st.session_state["messages"] = [
            {"role": "assistant", "content": welcome_text}
        ]

    # 3. Відображення історії
    for msg in st.session_state["messages"]:
        if msg["role"] == "user":
            avatar_icon = "👤"
        else:
            avatar_icon = "🤖"
            
        with st.chat_message(msg["role"], avatar=avatar_icon):
            st.markdown(msg["content"])

    # 4. Обробка вводу
    if prompt := st.chat_input("Напишіть ваше запитання..."):
        
        # A. Показуємо повідомлення користувача
        st.session_state["messages"].append({"role": "user", "content": prompt})
        with st.chat_message("user", avatar="👤"):
            st.markdown(prompt)

        # B. Відправка на n8n
        with st.chat_message("assistant", avatar="🤖"):
            message_placeholder = st.empty()
            
            with st.spinner("Аналізую дані..."):
                try:
                    # --- FORM PAYLOAD ---
                    payload = {
                        "query": prompt,
                        
                        # Дані користувача + РОЛЬ
                        "user_id": user.id if user else "guest",
                        "user_email": user.email if user else None,
                        "role": role,
                        
                        # Дані проекту
                        "project_id": proj.get("id"),
                        "project_name": proj.get("brand_name"),
                        "domain": proj.get("domain"),
                        "status": proj.get("status")
                    }

                    # 🔥 ЗАПИТ З ВИПРАВЛЕНИМ ЗАГОЛОВКОМ
                    response = requests.post(
                        N8N_CHAT_WEBHOOK, 
                        json=payload, 
                        headers=headers, 
                        timeout=60
                    )

                    if response.status_code == 200:
                        data = response.json()
                        bot_reply = data.get("output") or data.get("answer") or data.get("text")
                        
                        if not bot_reply:
                            bot_reply = f"⚠️ Отримана пуста відповідь. (Raw: {data})"
                    elif response.status_code == 403:
                        bot_reply = "⛔ Помилка 403: Доступ заборонено. Перевірте назву заголовка (Header Name) в n8n."
                    elif response.status_code == 404:
                        bot_reply = "⚠️ Помилка 404: Вебхук не знайдено. Переконайтеся, що Worklow в n8n увімкнено (Active)."
                    else:
                        bot_reply = f"⚠️ Помилка сервера: {response.status_code}"

                except Exception as e:
                    bot_reply = f"⚠️ Помилка з'єднання: {e}"

                # C. Вивід відповіді
                message_placeholder.markdown(bot_reply)
        
        # D. Збереження відповіді
        st.session_state["messages"].append({"role": "assistant", "content": bot_reply})
        
            
def main():
    # 1. Session Check
    check_session()

    # 2. If not logged in -> Show Auth Page
    if not st.session_state.get("user"):
        show_auth_page()  # <--- CHANGED THIS LINE
        return

    # 3. ОТРИМАННЯ ДАНИХ ПРОЕКТУ
    # Якщо користувач залогінений, але проект ще не завантажено в сесію - пробуємо знайти
    if not st.session_state.get("current_project"):
        try:
            user_id = st.session_state["user"].id
            # Шукаємо проект користувача
            resp = supabase.table("projects").select("*").eq("user_id", user_id).execute()
            if resp.data:
                # Якщо знайшли - записуємо в сесію (беремо перший)
                st.session_state["current_project"] = resp.data[0]
                st.rerun() # Перезавантажуємо, щоб оновити інтерфейс
        except Exception:
            pass

    # 4. ЛОГІКА ONBOARDING
    # Якщо проекту все ще немає (і це не адмін, бо адмін може не мати свого проекту)
    if st.session_state.get("current_project") is None and st.session_state.get("role") != "admin":
        # Показуємо кнопку виходу в сайдбарі (щоб не застряг)
        with st.sidebar:
            st.image("https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png", width=150) # Або текст
            if st.button("Вийти"):
                logout()
        
        # Запускаємо Майстер створення
        onboarding_wizard()
    
    # 5. ОСНОВНИЙ ДОДАТОК
    else:
        # Меню
        page = sidebar_menu()

        if page == "Дашборд":
            show_dashboard()
        elif page == "Перелік запитів":
            show_keywords_page()
        elif page == "Джерела":
            show_sources_page()
        elif page == "Конкуренти":
            show_competitors_page()
        elif page == "Рекомендації":
            show_recommendations_page()
        elif page == "GPT-Visibility":
            show_chat_page()
        elif page == "Адмін":
            if st.session_state.get("role") == "admin":
                show_admin_page()
            else:
                st.error("Доступ заборонено.")

if __name__ == "__main__":
    main()
