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
import uuid


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
N8N_RECO_URL = "https://virshi.app.n8n.cloud/webhook/recommendations"  
N8N_CHAT_WEBHOOK = "https://virshi.app.n8n.cloud/webhook/webhook/chat-bot" 


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
    ОНОВЛЕНО: Додано header авторизації (virshi-auth), щоб виправити помилку 403.
    """
    try:
        payload = {
            "brand": brand,
            "domain": domain,
            "industry": industry,
            "products": products,
        }

        # 🔥 ВАЖЛИВО: Додаємо заголовок авторизації
        headers = {
            "virshi-auth": "hi@virshi.ai2025"
        }

        # Передаємо headers у запит
        response = requests.post(N8N_GEN_URL, json=payload, headers=headers, timeout=20)

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
    ВЕРСІЯ: FIX METRICS (CLEAN DATA).
    1. Очищує official_assets (видаляє https://, www) для коректного підрахунку.
    2. Зберігає логіку Trial (блокування повторів).
    """
    import requests
    import streamlit as st
    
    # --- 1. ПІДКЛЮЧЕННЯ ДО БД ---
    if 'supabase' in st.session_state:
        supabase = st.session_state['supabase']
    elif 'supabase' in globals():
        supabase = globals()['supabase']
    else:
        st.error("🚨 Помилка: Немає підключення до БД.")
        return False

    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }

    # 2. ОТРИМАННЯ СТАТУСУ
    current_proj = st.session_state.get("current_project")
    
    status = "trial"
    if current_proj and current_proj.get("id") == project_id:
        status = current_proj.get("status", "trial")
    
    if status == "blocked":
        st.error("⛔ Проект заблоковано. Зверніться до адміністратора.")
        return False

    if not models:
        models = ["Perplexity"]

    # ==========================================
    # 🔥 ЛОГІКА ТРІАЛУ (ЗАХИСТ)
    # ==========================================
    if status == "trial":
        is_only_gemini = True
        for m in models:
            if "Gemini" not in m and "gemini" not in m:
                is_only_gemini = False
                break
        
        if not is_only_gemini:
            st.warning("🔒 У статусі TRIAL доступний аналіз лише через Google Gemini.")
            return False

        try:
            # Перевірка на повторний запуск
            existing = supabase.table("scan_results")\
                .select("id", count="exact")\
                .eq("project_id", project_id)\
                .limit(1)\
                .execute()
            
            if existing.data or (existing.count and existing.count > 0):
                st.error("⛔ Аналіз неможливий у статусі TRIAL (ліміт вичерпано). Будь ласка, зверніться в техпідтримку на пошту hi@virshi.ai, щоб отримати статус ACTIVE.")
                return False
        except Exception as e:
            print(f"Trial check error: {e}")

    try:
        user = st.session_state.get("user")
        user_email = user.email if user else "no-reply@virshi.ai"
        
        if isinstance(keywords, str):
            keywords = [keywords]

        success_count = 0

        # --- 3. ОТРИМАННЯ ТА ЧИСТКА WHITELIST (ВАЖЛИВО!) ---
        clean_assets = []
        try:
            assets_resp = supabase.table("official_assets")\
                .select("domain_or_url")\
                .eq("project_id", project_id)\
                .execute()
            
            if assets_resp.data:
                for item in assets_resp.data:
                    raw_url = item.get("domain_or_url", "").lower().strip()
                    # Видаляємо сміття, щоб n8n міг знайти цей домен у тексті
                    clean = raw_url.replace("https://", "").replace("http://", "").replace("www.", "").rstrip("/")
                    if clean:
                        clean_assets.append(clean)
        except Exception as e:
            print(f"Error fetching assets: {e}")
            clean_assets = []

        headers = {"virshi-auth": "hi@virshi.ai2025"}

        # 4. ВІДПРАВКА
        for ui_model_name in models:
            tech_model_id = MODEL_MAPPING.get(ui_model_name, ui_model_name)

            payload = {
                "project_id": project_id,
                "keywords": keywords, 
                "brand_name": brand_name,
                "user_email": user_email,
                "provider": tech_model_id,
                "models": [tech_model_id],
                
                # 🔥 ВІДПРАВЛЯЄМО ЧИСТІ ДОМЕНИ
                "official_assets": clean_assets 
            }
            
            try:
                # Переконайтеся, що змінна N8N_ANALYZE_URL доступна
                response = requests.post(
                    N8N_ANALYZE_URL, 
                    json=payload, 
                    headers=headers, 
                    timeout=20
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


def trigger_ai_recommendation(user, project, category, context_text):
    """
    Відправляє запит на AI для генерації HTML-звіту.
    """
    import requests
    from datetime import datetime
    
    headers = {
        "virshi-auth": "hi@virshi.ai2025"
    }
    
    # Формуємо повний payload
    payload = {
        "timestamp": datetime.now().isoformat(),
        # Інформація про користувача
        "user_id": user.id if user else "unknown",
        "user_email": user.email if user else "unknown",
        
        # Інформація про проект
        "project_id": project.get("id"),
        "brand_name": project.get("brand_name"),
        "domain": project.get("domain"),
        
        # Деталі запиту
        "category": category, 
        "request_context": context_text,
        "request_type": "html_report"
    }
    
    try:
        response = requests.post(N8N_RECO_URL, json=payload, headers=headers, timeout=120)
        
        if response.status_code == 200:
            # Спробуємо розпарсити JSON, якщо n8n повертає об'єкт
            try:
                data = response.json()
                # Шукаємо HTML у різних полях
                return data.get("html") or data.get("output") or data.get("report") or str(data)
            except:
                # Якщо повернувся просто текст/html
                return response.text
        else:
            return f"<p style='color:red; font-weight:bold;'>Error from AI Provider: {response.status_code}</p>"
            
    except Exception as e:
        return f"<p style='color:red; font-weight:bold;'>Connection Error: {e}</p>"


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
    Майстер створення проекту.
    ВЕРСІЯ: REGION SELECT + TIMEOUT FIX.
    1. Регіон: Випадаючий список (Ukraine, USA, Europe, Global).
    2. База даних: Регіон записується динамічно.
    """
    import requests
    import time
    
    # Ініціалізація змінних сесії
    if "onboarding_stage" not in st.session_state:
        st.session_state["onboarding_stage"] = 2
        st.session_state["generated_prompts"] = []
    
    # CSS для зелених номерів
    st.markdown("""
    <style>
        .green-number-small {
            background-color: #00C896;
            color: white;
            width: 24px;
            height: 24px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: bold;
            font-size: 12px;
            margin-top: 8px;
        }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown("## 🚀 Налаштування Проекту")
    step = st.session_state.get("onboarding_step", 2) 

    with st.container(border=True):
        # ========================================================
        # КРОК 1: ВВІД ДАНИХ
        # ========================================================
        if step == 2:
            st.subheader("Крок 1: Введіть дані про ваш бренд")
            c1, c2 = st.columns(2)
            with c1:
                brand = st.text_input("Назва бренду", placeholder="Monobank", value=st.session_state.get("temp_brand", ""))
                industry = st.text_input("Галузь бренду / ніша", placeholder="Фінтех", value=st.session_state.get("temp_industry", ""))
            with c2:
                domain = st.text_input("Домен (офіційний сайт)", placeholder="monobank.ua", value=st.session_state.get("temp_domain", ""))
                
                # 🔥 UPDATE: Випадаючий список замість фіксованого тексту
                region_options = ["Ukraine", "USA", "Europe", "Global"]
                # Якщо раніше вже обирали, беремо збережене, інакше дефолт Ukraine (0 індекс)
                saved_region = st.session_state.get("temp_region", "Ukraine")
                try:
                    idx = region_options.index(saved_region)
                except:
                    idx = 0
                
                region = st.selectbox("Регіон", options=region_options, index=idx)

            products = st.text_area("Продукти / Послуги", value=st.session_state.get("temp_products", ""))

            if st.button("Згенерувати запити"):
                if brand and domain and industry and products:
                    st.session_state["temp_brand"] = brand
                    st.session_state["temp_domain"] = domain
                    st.session_state["temp_industry"] = industry
                    st.session_state["temp_products"] = products
                    # 🔥 UPDATE: Зберігаємо обраний регіон
                    st.session_state["temp_region"] = region

                    with st.spinner("Генеруємо релевантні запити..."):
                        # Виклик оновленої функції з таймаутом 60с
                        prompts = n8n_generate_prompts(brand, domain, industry, products)
                        if prompts:
                            st.session_state["generated_prompts"] = prompts
                            st.session_state["onboarding_step"] = 3
                            st.rerun()
                        # Якщо помилка таймауту, повідомлення виведе сама функція n8n_generate_prompts
                else:
                    st.warning("⚠️ Будь ласка, заповніть всі поля.")

        # ========================================================
        # КРОК 2: ПЕРЕВІРКА ТА ЗАПУСК
        # ========================================================
        elif step == 3:
            st.subheader("Крок 2: Перевірка та запуск")
            
            prompts_list = st.session_state.get("generated_prompts", [])
            
            if not prompts_list:
                st.warning("Список запитів порожній.")
                if st.button("Назад"):
                    st.session_state["onboarding_step"] = 2
                    st.rerun()
                return

            st.markdown("Перевірте та відредагуйте запити перед запуском:")
            st.write("") 

            selected_indices = []

            # --- ЦИКЛ ВИВОДУ КАРТОК З РЕДАГУВАННЯМ ---
            for i, kw in enumerate(prompts_list):
                edit_key = f"edit_mode_row_{i}"
                if edit_key not in st.session_state:
                    st.session_state[edit_key] = False

                with st.container(border=True):
                    c_chk, c_num, c_text, c_btn = st.columns([0.5, 0.5, 8, 1])
                    
                    # 1. Чекбокс
                    with c_chk:
                        st.write("") 
                        if st.checkbox("", value=True, key=f"chk_final_{i}"):
                            selected_indices.append(i)
                    
                    # 2. Номер
                    with c_num:
                        st.markdown(f"<div class='green-number-small'>{i+1}</div>", unsafe_allow_html=True)

                    # 3. Текст / Поле
                    with c_text:
                        if st.session_state[edit_key]:
                            new_val = st.text_input("", value=kw, key=f"input_kw_{i}", label_visibility="collapsed")
                        else:
                            st.markdown(f"<div style='padding-top: 8px; font-size: 15px;'>{kw}</div>", unsafe_allow_html=True)

                    # 4. Кнопка
                    with c_btn:
                        st.write("") 
                        if st.session_state[edit_key]:
                            if st.button("💾", key=f"save_kw_{i}", help="Зберегти"):
                                st.session_state["generated_prompts"][i] = new_val
                                st.session_state[edit_key] = False
                                st.rerun()
                        else:
                            if st.button("✏️", key=f"edit_kw_{i}", help="Редагувати"):
                                st.session_state[edit_key] = True
                                st.rerun()

            final_kws_to_send = [st.session_state["generated_prompts"][idx] for idx in selected_indices]
            
            st.divider()
            c_info, c_launch = st.columns([2, 1])
            with c_info:
                st.markdown(f"**Готово до запуску:** {len(final_kws_to_send)} запитів")
            
            with c_launch:
                if st.button("🚀 Зберегти та Запустити аналіз", type="primary", use_container_width=True):
                    if final_kws_to_send:
                        try:
                            # 1. ЗБИРАЄМО ДАНІ
                            user_id = st.session_state["user"].id
                            brand_name = st.session_state.get("temp_brand")
                            domain_name = st.session_state.get("temp_domain")
                            # 🔥 UPDATE: Беремо регіон зі стейту
                            region_val = st.session_state.get("temp_region", "Ukraine")
                            
                            # 2. СТВОРЮЄМО ПРОЕКТ
                            res = supabase.table("projects").insert({
                                "user_id": user_id, 
                                "brand_name": brand_name,
                                "domain": domain_name,
                                "region": region_val,  # <--- Записуємо обраний регіон
                                "status": "trial"
                            }).execute()

                            if res.data:
                                proj_data = res.data[0]
                                proj_id = proj_data["id"]
                                
                                st.session_state["current_project"] = proj_data

                                # 3. ЗАПИСУЄМО WHITELIST
                                clean_d = domain_name.replace("https://", "").replace("http://", "").strip().rstrip("/")
                                try:
                                    supabase.table("official_assets").insert({
                                        "project_id": proj_id, 
                                        "domain_or_url": clean_d,
                                        "type": "website"
                                    }).execute()
                                except Exception:
                                    pass 

                                # 4. ЗАПИСУЄМО KEYWORDS
                                kws_data = [{"project_id": proj_id, "keyword_text": kw, "is_active": True} for kw in final_kws_to_send]
                                supabase.table("keywords").insert(kws_data).execute()
                                
                                # 5. ВІДПРАВКА НА N8N
                                my_bar = st.progress(0, text="Ініціалізація AI-аналітика...")
                                total_kws = len(final_kws_to_send)

                                for i, single_kw in enumerate(final_kws_to_send):
                                    progress_pct = (i + 1) / total_kws
                                    my_bar.progress(progress_pct, text=f"Аналіз запиту: {single_kw}...")
                                    
                                    n8n_trigger_analysis(
                                        project_id=proj_id, 
                                        keywords=[single_kw],     
                                        brand_name=brand_name,
                                        models=["Google Gemini"]  
                                    )
                                    time.sleep(0.5) 

                                my_bar.progress(1.0, text="✅ Проект створено успішно!")
                                time.sleep(1)

                                # Фінал
                                st.session_state["onboarding_step"] = 2 
                                st.success("Успіх!")
                                st.rerun()

                        except Exception as e:
                            st.error(f"Помилка створення проекту: {e}")
                    else:
                        st.warning("Оберіть хоча б один запит.")
                    
# =========================
# 6. DASHBOARD
# =========================

def show_competitors_page():
    """
    Сторінка глибокого конкурентного аналізу.
    ВЕРСІЯ: FINAL VISUALIZATION (Stacked Bars & Colors).
    1. Тональність: Графік Stacked Bar (🔴/⚪/🟢) для кожного бренду.
    2. Середня позиція: Топ-10 + Цільовий. Графік: Зелений (ми) vs Сірий (інші). Інверсія осі Y.
    3. Ліміт рядків: Мінімум 20.
    """
    import pandas as pd
    import plotly.express as px
    import streamlit as st
    import io
    import math

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
    
    OFFICIAL_BRAND_NAME = proj.get("brand_name", "My Brand")

    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }

    # --- Ініціалізація станів пагінації ---
    if 'cp_page_list' not in st.session_state: st.session_state.cp_page_list = 1
    if 'cp_page_freq' not in st.session_state: st.session_state.cp_page_freq = 1
    if 'cp_page_sent' not in st.session_state: st.session_state.cp_page_sent = 1
    if 'cp_page_rank' not in st.session_state: st.session_state.cp_page_rank = 1

    def reset_p_list(): st.session_state.cp_page_list = 1
    def reset_p_freq(): st.session_state.cp_page_freq = 1
    def reset_p_sent(): st.session_state.cp_page_sent = 1
    def reset_p_rank(): st.session_state.cp_page_rank = 1

    st.title("👥 Аналіз Конкурентів")

    # --- 1. ЗАВАНТАЖЕННЯ ДАНИХ ---
    try:
        scans_resp = supabase.table("scan_results")\
            .select("id, provider, keyword_id, created_at")\
            .eq("project_id", proj["id"])\
            .execute()
        
        if not scans_resp.data:
            st.info("Даних немає. Запустіть сканування.")
            return
            
        df_scans = pd.DataFrame(scans_resp.data)
        
        kw_resp = supabase.table("keywords").select("id, keyword_text").eq("project_id", proj["id"]).execute()
        kw_map = {k['id']: k['keyword_text'] for k in kw_resp.data}
        df_scans['keyword_text'] = df_scans['keyword_id'].map(kw_map)

        scan_ids = df_scans['id'].tolist()
        mentions_resp = supabase.table("brand_mentions")\
            .select("*")\
            .in_("scan_result_id", scan_ids)\
            .execute()
        
        if not mentions_resp.data:
            st.info("Брендів не знайдено.")
            return

        df_mentions = pd.DataFrame(mentions_resp.data)
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

    if sel_tech_models:
        mask_model = df_full['provider'].apply(lambda x: any(t in str(x) for t in sel_tech_models))
    else:
        mask_model = df_full['provider'].apply(lambda x: False)

    if sel_kws:
        mask_kw = df_full['keyword_text'].isin(sel_kws)
    else:
        mask_kw = df_full['keyword_text'].apply(lambda x: False)

    df_filtered = df_full[mask_model & mask_kw].copy()

    if df_filtered.empty:
        st.warning("За обраними фільтрами даних немає.")
        return

    # --- 3. АГРЕГАЦІЯ ---
    mask_target = df_filtered['is_my_brand'] == True
    if mask_target.any():
        df_filtered.loc[mask_target, 'brand_name'] = OFFICIAL_BRAND_NAME

    def sentiment_to_score(s):
        if s == 'Позитивний': return 100
        if s == 'Негативний': return 0
        return 50
    
    df_filtered['sent_score_num'] = df_filtered['sentiment_score'].apply(sentiment_to_score)

    stats = df_filtered.groupby('brand_name').agg(
        Mentions=('id_x', 'count'),
        Avg_Rank=('rank_position', 'mean'),
        Avg_Sentiment_Num=('sent_score_num', 'mean'),
        Is_My_Brand=('is_my_brand', 'max')
    ).reset_index()

    # Детальна тональність
    sent_counts = df_filtered.groupby(['brand_name', 'sentiment_score']).size().unstack(fill_value=0)
    for col in ['Негативний', 'Нейтральний', 'Позитивний']:
        if col not in sent_counts.columns: sent_counts[col] = 0
            
    sent_counts['Total'] = sent_counts.sum(axis=1)
    
    # Відсотки
    sent_counts['Neg_Pct'] = (sent_counts['Негативний'] / sent_counts['Total'] * 100).fillna(0).astype(int)
    sent_counts['Neu_Pct'] = (sent_counts['Нейтральний'] / sent_counts['Total'] * 100).fillna(0).astype(int)
    sent_counts['Pos_Pct'] = (sent_counts['Позитивний'] / sent_counts['Total'] * 100).fillna(0).astype(int)

    # Строка для таблиці
    sent_counts['Тональність_Str'] = sent_counts.apply(
        lambda x: f"🔴 {x['Neg_Pct']}%   ⚪ {x['Neu_Pct']}%   🟢 {x['Pos_Pct']}%", axis=1
    )

    stats = stats.merge(sent_counts[['Тональність_Str', 'Neg_Pct', 'Neu_Pct', 'Pos_Pct']], on='brand_name', how='left')
    stats['Тональність_Str'] = stats['Тональність_Str'].fillna("🔴 0% ⚪ 0% 🟢 0%")
    stats[['Neg_Pct', 'Neu_Pct', 'Pos_Pct']] = stats[['Neg_Pct', 'Neu_Pct', 'Pos_Pct']].fillna(0)

    # --- ЛОГІКА TOP-N (Helper Function) ---
    def set_top_n_flag(df, sort_col, n=15, ascending=False):
        """
        Встановлює 'Show' = True для Top N брендів.
        Гарантовано включає цільовий бренд.
        """
        df = df.sort_values(sort_col, ascending=ascending).reset_index(drop=True)
        df['Show'] = False
        
        top_indices = df.index[:n].tolist()
        target_idx = df[df['brand_name'] == OFFICIAL_BRAND_NAME].index
        
        if not target_idx.empty:
            t_idx = target_idx[0]
            if t_idx not in top_indices:
                if len(top_indices) == n:
                    top_indices.pop()
                top_indices.append(t_idx)
        
        df.loc[top_indices, 'Show'] = True
        return df

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
        c_head, c_search, c_rows = st.columns([2, 2, 1])
        with c_head: st.markdown("##### 📋 Зведена таблиця")
        with c_search: search_list = st.text_input("🔍 Пошук бренду", key="s_list", on_change=reset_p_list)
        # Мінімум 20 рядків
        with c_rows: rows_list = st.selectbox("Рядків", [20, 50, 100, 200], key="r_list", on_change=reset_p_list)
        
        display_df = stats.copy().sort_values('Mentions', ascending=False).reset_index(drop=True)
        display_df.index = display_df.index + 1
        display_df.index.name = '#'
        display_df['Сер. Позиція'] = display_df['Avg_Rank'].apply(lambda x: f"#{x:.1f}")

        if search_list:
            display_df = display_df[display_df['brand_name'].astype(str).str.contains(search_list, case=False, na=False)]

        total_rows = len(display_df)
        total_pages = math.ceil(total_rows / rows_list)
        if st.session_state.cp_page_list > total_pages: st.session_state.cp_page_list = max(1, total_pages)
        curr_p = st.session_state.cp_page_list
        start_idx = (curr_p - 1) * rows_list
        end_idx = start_idx + rows_list
        df_page = display_df.iloc[start_idx:end_idx].copy()

        nc1, nc2, nc3 = st.columns([1, 2, 1])
        with nc1:
            if curr_p > 1: 
                if st.button("⬅️ Попередня", key="prev_list_t"): st.session_state.cp_page_list -= 1; st.rerun()
        with nc2: st.caption(f"Стор. {curr_p} з {total_pages} (Всього: {total_rows})")
        with nc3:
            if curr_p < total_pages:
                if st.button("Наступна ➡️", key="next_list_t"): st.session_state.cp_page_list += 1; st.rerun()

        def highlight_target_row(row):
            if row['brand_name'] == OFFICIAL_BRAND_NAME:
                return ['background-color: #d4edda; color: #155724; font-weight: bold'] * len(row)
            return [''] * len(row)

        cols_to_show = ['brand_name', 'Mentions', 'Сер. Позиція', 'Тональність_Str']
        styled_df = df_page[cols_to_show].style.apply(highlight_target_row, axis=1)

        dynamic_h = (len(df_page) * 35) + 38
        st.dataframe(
            styled_df,
            use_container_width=True,
            height=dynamic_h,
            column_config={
                "brand_name": "Бренд",
                "Mentions": st.column_config.ProgressColumn("Згадок", format="%d", min_value=0, max_value=int(stats['Mentions'].max())),
                "Сер. Позиція": st.column_config.TextColumn("Сер. Позиція", width="small"),
                "Тональність_Str": st.column_config.TextColumn("Тональність", width="medium")
            }
        )

        if total_rows > 20:
            bc1, bc2, bc3 = st.columns([1, 2, 1])
            with bc1:
                if curr_p > 1: 
                    if st.button("⬅️ Попередня", key="prev_list_b"): st.session_state.cp_page_list -= 1; st.rerun()
            with bc3:
                if curr_p < total_pages:
                    if st.button("Наступна ➡️", key="next_list_b"): st.session_state.cp_page_list += 1; st.rerun()

    # === TAB 2: ЧАСТОТА ЗГАДКИ ===
    with tab_freq:
        c_head, c_search, c_rows = st.columns([2, 2, 1])
        with c_head: st.markdown("##### 📊 Частота згадки (Топ-15)")
        with c_search: search_freq = st.text_input("🔍 Пошук бренду", key="s_freq", on_change=reset_p_freq)
        with c_rows: rows_freq = st.selectbox("Рядків", [20, 50, 100, 200], key="r_freq", on_change=reset_p_freq)
        
        df_for_freq = stats.copy()
        df_for_freq['Display_Name'] = df_for_freq.apply(
            lambda x: f"🟢 {x['brand_name']}" if x['brand_name'] == OFFICIAL_BRAND_NAME else x['brand_name'], axis=1
        )
        # Топ-15
        df_for_freq = set_top_n_flag(df_for_freq, 'Mentions', n=15, ascending=False)
        
        if search_freq:
            df_for_freq = df_for_freq[df_for_freq['brand_name'].astype(str).str.contains(search_freq, case=False, na=False)]

        col_table, col_chart = st.columns([1.8, 2.2])

        with col_table:
            total_rows = len(df_for_freq)
            total_pages = math.ceil(total_rows / rows_freq)
            if st.session_state.cp_page_freq > total_pages: st.session_state.cp_page_freq = max(1, total_pages)
            curr_p = st.session_state.cp_page_freq
            start_idx = (curr_p - 1) * rows_freq
            end_idx = start_idx + rows_freq
            df_page = df_for_freq.iloc[start_idx:end_idx]

            nc1, nc2, nc3 = st.columns([1, 2, 1])
            with nc1:
                if curr_p > 1: 
                    if st.button("⬅️", key="p_freq_t"): st.session_state.cp_page_freq -= 1; st.rerun()
            with nc2: st.caption(f"Стор. {curr_p}/{total_pages}")
            with nc3:
                if curr_p < total_pages: 
                    if st.button("➡️", key="n_freq_t"): st.session_state.cp_page_freq += 1; st.rerun()

            dynamic_h = (len(df_page) * 35) + 38
            edited_freq_df = st.data_editor(
                df_page[['Show', 'Display_Name', 'Mentions']],
                column_config={
                    "Show": st.column_config.CheckboxColumn("Відобразити", width="small"),
                    "Display_Name": st.column_config.TextColumn("Бренд", disabled=True),
                    "Mentions": st.column_config.ProgressColumn("Згадок", format="%d", min_value=0, max_value=int(stats['Mentions'].max())),
                },
                hide_index=True,
                use_container_width=True,
                height=dynamic_h,
                key=f"editor_freq_{curr_p}"
            )
            
            if total_rows > 20:
                bc1, bc2, bc3 = st.columns([1, 2, 1])
                with bc1:
                    if curr_p > 1: 
                        if st.button("⬅️", key="p_freq_b"): st.session_state.cp_page_freq -= 1; st.rerun()
                with bc3:
                    if curr_p < total_pages: 
                        if st.button("➡️", key="n_freq_b"): st.session_state.cp_page_freq += 1; st.rerun()

        with col_chart:
            chart_data = edited_freq_df[edited_freq_df['Show'] == True].copy()
            chart_data['Original_Name'] = chart_data['Display_Name'].apply(lambda x: x.replace("🟢 ", ""))
            
            if not chart_data.empty:
                # Додаємо колір: Зелений для нашого, Сірий/Тіл для інших
                chart_data['Color'] = chart_data['Original_Name'].apply(lambda x: '#00C896' if x == OFFICIAL_BRAND_NAME else '#90A4AE')
                
                fig = px.bar(
                    chart_data, 
                    x='Original_Name', 
                    y='Mentions',
                    text='Mentions'
                )
                fig.update_traces(marker_color=chart_data['Color'])
                fig.update_layout(xaxis_title="", yaxis_title="Кількість згадок", showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Оберіть бренди.")

    # === TAB 3: ТОНАЛЬНІСТЬ (STACKED BAR CHART) ===
    with tab_sent:
        c_head, c_search, c_rows = st.columns([2, 2, 1])
        with c_head: st.markdown("##### ⭐ Тональність (Топ-15)")
        with c_search: search_sent = st.text_input("🔍 Пошук бренду", key="s_sent", on_change=reset_p_sent)
        with c_rows: rows_sent = st.selectbox("Рядків", [20, 50, 100, 200], key="r_sent", on_change=reset_p_sent)
        
        df_for_sent = stats.copy()
        df_for_sent['Display_Name'] = df_for_sent.apply(
            lambda x: f"🟢 {x['brand_name']}" if x['brand_name'] == OFFICIAL_BRAND_NAME else x['brand_name'], axis=1
        )
        df_for_sent = set_top_n_flag(df_for_sent, 'Mentions', n=15, ascending=False)

        if search_sent:
            df_for_sent = df_for_sent[df_for_sent['brand_name'].astype(str).str.contains(search_sent, case=False, na=False)]

        col_table, col_chart = st.columns([1.8, 2.2])

        with col_table:
            total_rows = len(df_for_sent)
            total_pages = math.ceil(total_rows / rows_sent)
            if st.session_state.cp_page_sent > total_pages: st.session_state.cp_page_sent = max(1, total_pages)
            curr_p = st.session_state.cp_page_sent
            start_idx = (curr_p - 1) * rows_sent
            end_idx = start_idx + rows_sent
            df_page = df_for_sent.iloc[start_idx:end_idx]

            nc1, nc2, nc3 = st.columns([1, 2, 1])
            with nc1:
                if curr_p > 1: 
                    if st.button("⬅️", key="p_sent_t"): st.session_state.cp_page_sent -= 1; st.rerun()
            with nc2: st.caption(f"Стор. {curr_p}/{total_pages}")
            with nc3:
                if curr_p < total_pages: 
                    if st.button("➡️", key="n_sent_t"): st.session_state.cp_page_sent += 1; st.rerun()

            dynamic_h = (len(df_page) * 35) + 38
            edited_sent_df = st.data_editor(
                df_page[['Show', 'Display_Name', 'Тональність_Str']],
                column_config={
                    "Show": st.column_config.CheckboxColumn("Відобразити", width="small"),
                    "Display_Name": st.column_config.TextColumn("Бренд", disabled=True),
                    "Тональність_Str": st.column_config.TextColumn("Розподіл", disabled=True, width="medium"),
                },
                hide_index=True,
                use_container_width=True,
                height=dynamic_h,
                key=f"editor_sent_{curr_p}"
            )
            
            if total_rows > 20:
                bc1, bc2, bc3 = st.columns([1, 2, 1])
                with bc1:
                    if curr_p > 1: 
                        if st.button("⬅️", key="p_sent_b"): st.session_state.cp_page_sent -= 1; st.rerun()
                with bc3:
                    if curr_p < total_pages: 
                        if st.button("➡️", key="n_sent_b"): st.session_state.cp_page_sent += 1; st.rerun()

        with col_chart:
            # 🔥 БУДУЄМО ГРАФІК З НАКОПИЧЕННЯМ (STACKED)
            selected_rows = edited_sent_df[edited_sent_df['Show'] == True]
            selected_rows['Original_Name'] = selected_rows['Display_Name'].apply(lambda x: x.replace("🟢 ", ""))
            
            # Нам треба перетворити дані в "довгий" формат для Plotly (Brand | Sentiment | Value)
            # Беремо дані з таблиці stats, бо там є відсотки
            target_brands = selected_rows['Original_Name'].tolist()
            chart_data_src = stats[stats['brand_name'].isin(target_brands)].copy()
            
            if not chart_data_src.empty:
                # Мелтимо (розгортаємо) датафрейм
                df_melted = chart_data_src.melt(
                    id_vars=['brand_name'], 
                    value_vars=['Neg_Pct', 'Neu_Pct', 'Pos_Pct'], 
                    var_name='Sentiment_Type', 
                    value_name='Percentage'
                )
                
                # Перейменовуємо для легенди
                df_melted['Sentiment'] = df_melted['Sentiment_Type'].map({
                    'Neg_Pct': 'Негативна',
                    'Neu_Pct': 'Нейтральна',
                    'Pos_Pct': 'Позитивна'
                })
                
                # Карта кольорів
                color_map = {
                    "Негативна": "#FF5252", # Червоний
                    "Нейтральна": "#CFD8DC", # Світло-сірий
                    "Позитивна": "#00C896"   # Зелений
                }
                
                fig = px.bar(
                    df_melted,
                    x="brand_name",
                    y="Percentage",
                    color="Sentiment",
                    text="Percentage",
                    color_discrete_map=color_map,
                    # Порядок: Негатив внизу, Нейтрал, Позитив зверху (або як зручно)
                    category_orders={"Sentiment": ["Негативна", "Нейтральна", "Позитивна"]},
                    height=500
                )
                
                fig.update_traces(texttemplate='%{text}%', textposition='inside')
                fig.update_layout(
                    barmode='stack', # Робить один стовпчик з частинами
                    xaxis_title="", 
                    yaxis_title="Частка (%)", 
                    legend_title="",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Оберіть бренди.")

    # === TAB 4: СЕРЕДНЯ ПОЗИЦІЯ (TOP-10) ===
    with tab_rank:
        c_head, c_search, c_rows = st.columns([2, 2, 1])
        with c_head: st.markdown("##### 🏆 Середня позиція (Топ-10)")
        with c_search: search_rank = st.text_input("🔍 Пошук бренду", key="s_rank", on_change=reset_p_rank)
        with c_rows: rows_rank = st.selectbox("Рядків", [20, 50, 100, 200], key="r_rank", on_change=reset_p_rank)

        col_table, col_chart = st.columns([1.8, 2.2])

        with col_table:
            df_for_rank = stats.copy()
            df_for_rank['Display_Name'] = df_for_rank.apply(
                lambda x: f"🟢 {x['brand_name']}" if x['brand_name'] == OFFICIAL_BRAND_NAME else x['brand_name'], axis=1
            )
            # 🔥 Топ-10 (n=10)
            df_for_rank = set_top_n_flag(df_for_rank, 'Avg_Rank', n=10, ascending=True)

            if search_rank:
                df_for_rank = df_for_rank[df_for_rank['brand_name'].astype(str).str.contains(search_rank, case=False, na=False)]

            total_rows = len(df_for_rank)
            total_pages = math.ceil(total_rows / rows_rank)
            if st.session_state.cp_page_rank > total_pages: st.session_state.cp_page_rank = max(1, total_pages)
            curr_p = st.session_state.cp_page_rank
            start_idx = (curr_p - 1) * rows_rank
            end_idx = start_idx + rows_rank
            df_page = df_for_rank.iloc[start_idx:end_idx]

            nc1, nc2, nc3 = st.columns([1, 2, 1])
            with nc1:
                if curr_p > 1: 
                    if st.button("⬅️", key="p_rank_t"): st.session_state.cp_page_rank -= 1; st.rerun()
            with nc2: st.caption(f"Стор. {curr_p}/{total_pages}")
            with nc3:
                if curr_p < total_pages: 
                    if st.button("➡️", key="n_rank_t"): st.session_state.cp_page_rank += 1; st.rerun()

            dynamic_h = (len(df_page) * 35) + 38
            edited_rank_df = st.data_editor(
                df_page[['Show', 'Display_Name', 'Avg_Rank']],
                column_config={
                    "Show": st.column_config.CheckboxColumn("Відобразити", width="small"),
                    "Display_Name": st.column_config.TextColumn("Бренд", disabled=True),
                    "Avg_Rank": st.column_config.NumberColumn("Сер. Позиція", format="%.1f"),
                },
                hide_index=True,
                use_container_width=True,
                height=dynamic_h,
                key=f"editor_rank_{curr_p}"
            )
            
            if total_rows > 20:
                bc1, bc2, bc3 = st.columns([1, 2, 1])
                with bc1:
                    if curr_p > 1: 
                        if st.button("⬅️", key="p_rank_b"): st.session_state.cp_page_rank -= 1; st.rerun()
                with bc3:
                    if curr_p < total_pages: 
                        if st.button("➡️", key="n_rank_b"): st.session_state.cp_page_rank += 1; st.rerun()

        with col_chart:
            chart_data = edited_rank_df[edited_rank_df['Show'] == True].copy()
            chart_data['Original_Name'] = chart_data['Display_Name'].apply(lambda x: x.replace("🟢 ", ""))
            
            # Колір: Зелений (Ми) vs Сірий (Інші)
            chart_data['Color'] = chart_data['Original_Name'].apply(
                lambda x: '#00C896' if x == OFFICIAL_BRAND_NAME else '#B0BEC5'
            )

            if not chart_data.empty:
                # Бар чарт
                fig = px.bar(
                    chart_data, 
                    x='Original_Name', 
                    y='Avg_Rank',
                    text='Avg_Rank'
                )
                
                # Фарбуємо
                fig.update_traces(
                    marker_color=chart_data['Color'],
                    texttemplate='%{text:.1f}', 
                    textposition='outside'
                )
                
                fig.update_layout(
                    xaxis_title="", 
                    yaxis_title="Середня позиція (менше = краще)", 
                    showlegend=False
                )
                # 🔥 Інверсія осі Y, щоб 1 було зверху
                fig.update_yaxes(autorange="reversed")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Оберіть бренди.")

def show_recommendations_page():
    """
    Сторінка рекомендацій.
    ВЕРСІЯ: RENAMED FILES & BUTTONS.
    File prefix: "Recommendations_"
    Button label: "Завантажити Рекомендації"
    """
    import streamlit as st
    import pandas as pd
    import streamlit.components.v1 as components
    from datetime import datetime, timedelta

    # --- 1. ПІДКЛЮЧЕННЯ ---
    if 'supabase' in st.session_state:
        supabase = st.session_state['supabase']
    elif 'supabase' in globals():
        supabase = globals()['supabase']
    else:
        st.error("🚨 DB Error: Немає з'єднання з базою даних.")
        return

    proj = st.session_state.get("current_project")
    user = st.session_state.get("user")
    
    if not proj:
        st.info("Спочатку оберіть проект.")
        return

    st.title(f"💡 Центр рекомендацій: {proj.get('brand_name')}")

    # --- 2. КАТЕГОРІЇ ---
    CATEGORIES = {
        "Digital": {
            "title": "Digital & Technical GEO",
            "desc": "Технічна оптимізація екосистеми бренду для алгоритмів AI.",
            "value": "LLM (ChatGPT, Gemini) — це програми. Якщо сайт технічно складний для них, вони його ігнорують. Ми аналізуємо код, розмітку Schema.org та доступність для ботів.",
            "prompt_context": "Analyze technical SEO, Schema markup, site structure, and data accessibility for LLM crawling. Focus on Technical GEO factors."
        },
        "Content": {
            "title": "Content Strategy",
            "desc": "Створення контенту, який AI захоче цитувати.",
            "value": "AI любить факти і структуру. Ми дамо план: які статті писати і як їх оформлювати, щоб стати 'джерелом істини' для нейромереж.",
            "prompt_context": "Generate content strategy optimized for Generative Search. Focus on answer structure, NLP-friendly formats, and topical authority."
        },
        "PR": {
            "title": "PR & Brand Authority",
            "desc": "Побудова авторитету через зовнішні джерела.",
            "value": "AI довіряє тому, про що пишуть авторитетні медіа. Ми визначимо, де вам треба з'явитися (Wiki, ЗМІ), щоб алгоритми вважали вас лідером.",
            "prompt_context": "Analyze brand authority signals, mentions in tier-1 media, and external trust factors influencing LLM perception."
        },
        "Social": {
            "title": "Social Media & UGC",
            "desc": "Вплив соціальних сигналів на видачу.",
            "value": "Gemini та Perplexity читають Reddit, LinkedIn та X у реальному часі. Ми покажемо, як керувати дискусією там, щоб AI бачив позитив.",
            "prompt_context": "Analyze social signals, User Generated Content (Reddit, LinkedIn, Reviews), and their impact on real-time AI answers."
        }
    }

    main_tab, history_tab = st.tabs(["🚀 Замовити рекомендацію", "📚 Історія рекомендацій"])

    # Підготовка безпечної назви бренду для файлів
    safe_brand_name = proj.get('brand_name', 'Brand').replace(" ", "_")

    # ========================================================
    # TAB 1: ЗАМОВЛЕННЯ
    # ========================================================
    with main_tab:
        st.markdown("Оберіть напрямок, щоб отримати стратегію **Generative Engine Optimization**.")
        
        cat_names = list(CATEGORIES.keys())
        cat_tabs = st.tabs([CATEGORIES[c]["title"] for c in cat_names])

        for idx, cat_key in enumerate(cat_names):
            info = CATEGORIES[cat_key]
            with cat_tabs[idx]:
                with st.container(border=True):
                    st.subheader(info["title"])
                    st.markdown(f"**Що це:** {info['desc']}")
                    st.info(f"💎 **Навіщо це вам:**\n\n{info['value']}")
                    st.write("") 
                    
                    # Кнопка генерації
                    btn_label = f"✨ Отримати рекомендації ({info['title']})"
                    
                    if st.button(btn_label, key=f"btn_rec_{cat_key}", type="primary", use_container_width=True):
                        
                        if proj.get('status') == 'blocked':
                            st.error("Проект заблоковано.")
                        else:
                            st.warning("⏳ Розпочато формування рекомендацій. Будь ласка, не закривайте сторінку і дочекайтеся завершення (це може зайняти до 60 секунд).")
                            
                            with st.spinner("Аналіз даних та генерація звіту..."):
                                if 'trigger_ai_recommendation' in globals():
                                    html_res = trigger_ai_recommendation(
                                        user=user, project=proj, category=info["title"], context_text=info["prompt_context"]
                                    )
                                    try:
                                        supabase.table("strategy_reports").insert({
                                            "project_id": proj["id"], 
                                            "category": cat_key, 
                                            "html_content": html_res, 
                                            "created_at": datetime.now().isoformat()
                                        }).execute()
                                        
                                        st.success("✅ Рекомендації успішно сформовано!")
                                        st.markdown(f"""
                                            <div style="padding:15px; border:1px solid #00C896; border-radius:5px; background-color:#f0fff4;">
                                                <p>Ваш звіт збережено. Перейдіть у вкладку <b>"Історія рекомендацій"</b>, щоб переглянути його.</p>
                                            </div>
                                        """, unsafe_allow_html=True)
                                        
                                    except Exception as e:
                                        st.error(f"Помилка збереження в БД: {e}")
                                        with st.expander("Резервний перегляд", expanded=True):
                                            components.html(html_res, height=600, scrolling=True)
                                            # Кнопка скачування (Резервна)
                                            st.download_button(
                                                "📥 Завантажити Рекомендації", 
                                                html_res, 
                                                file_name=f"Recommendations_{cat_key}_{safe_brand_name}.html", 
                                                mime="text/html"
                                            )
                                else:
                                    st.error("Функція trigger_ai_recommendation не знайдена.")

    # ========================================================
    # TAB 2: ІСТОРІЯ
    # ========================================================
    with history_tab:
        c_h1, c_h2 = st.columns(2)
        with c_h1:
            sel_cat_hist = st.multiselect("Фільтр по категорії", list(CATEGORIES.keys()), default=[])
        with c_h2:
            date_filter_options = ["Весь час", "Сьогодні", "Останні 7 днів", "Останні 30 днів"]
            sel_date_range = st.selectbox("Період", date_filter_options)

        try:
            query = supabase.table("strategy_reports").select("*").eq("project_id", proj["id"]).order("created_at", desc=True)
            hist_resp = query.execute()
            reports = hist_resp.data if hist_resp.data else []
            
            if reports:
                df_rep = pd.DataFrame(reports)
                df_rep['created_at_dt'] = pd.to_datetime(df_rep['created_at'])
                
                # Фільтри
                if sel_cat_hist:
                    df_rep = df_rep[df_rep['category'].isin(sel_cat_hist)]
                
                now = datetime.now(df_rep['created_at_dt'].dt.tz)
                
                if sel_date_range == "Сьогодні":
                    df_rep = df_rep[df_rep['created_at_dt'].dt.date == now.date()]
                elif sel_date_range == "Останні 7 днів":
                    df_rep = df_rep[df_rep['created_at_dt'] >= (now - timedelta(days=7))]
                elif sel_date_range == "Останні 30 днів":
                    df_rep = df_rep[df_rep['created_at_dt'] >= (now - timedelta(days=30))]
                
                if df_rep.empty:
                    st.info("За обраними критеріями звітів не знайдено.")
                else:
                    for _, row in df_rep.iterrows():
                        cat_nice = CATEGORIES.get(row['category'], {}).get('title', row['category'])
                        try: date_str = row['created_at'][:16].replace('T', ' ')
                        except: date_str = "-"
                        
                        # Формуємо красиву дату для файлу (наприклад: 2023-10-25_14-30)
                        date_file = date_str.replace(" ", "_").replace(":", "-")

                        with st.expander(f"📑 {cat_nice} | {date_str}"):
                            c_dl, c_del = st.columns([4, 1])
                            
                            with c_dl:
                                # 🔥 Нова назва файлу: Recommendations_Category_Brand_Date.html
                                file_n = f"Recommendations_{row['category']}_{safe_brand_name}_{date_file}.html"
                                
                                # 🔥 Нова назва кнопки (без .html)
                                st.download_button(
                                    label="📥 Завантажити Рекомендації", 
                                    data=row['html_content'], 
                                    file_name=file_n, 
                                    mime="text/html",
                                    key=f"dl_hist_{row['id']}"
                                )
                            
                            with c_del:
                                del_key = f"confirm_del_{row['id']}"
                                if del_key not in st.session_state:
                                    st.session_state[del_key] = False

                                if not st.session_state[del_key]:
                                    if st.button("🗑️", key=f"pre_del_{row['id']}", help="Видалити звіт"):
                                        st.session_state[del_key] = True
                                        st.rerun()
                                else:
                                    col_yes, col_no = st.columns(2)
                                    if col_yes.button("✅", key=f"yes_{row['id']}"):
                                        supabase.table("strategy_reports").delete().eq("id", row['id']).execute()
                                        st.session_state[del_key] = False
                                        st.rerun()
                                    if col_no.button("❌", key=f"no_{row['id']}"):
                                        st.session_state[del_key] = False
                                        st.rerun()
                            
                            st.divider()
                            components.html(row['html_content'], height=500, scrolling=True)
            else:
                st.info("Історія рекомендацій порожня. Згенеруйте першу стратегію.")
                
        except Exception as e:
            st.warning(f"Неможливо завантажити історію: {e}")

def show_faq_page():
    """
    Сторінка FAQ & Support.
    ВЕРСІЯ: TOP-20 QUESTIONS + CONTACTS.
    """
    import streamlit as st

    st.title("❓ Центр підтримки та FAQ")

    # --- 1. Цінність платформи ---
    with st.container(border=True):
        st.markdown("### 🚀 Про Virshi.ai Visibility Platform")
        st.markdown("""
        **Virshi.ai** — це інструмент нового покоління для **GEO (Generative Engine Optimization)**. 
        Ми допомагаємо брендам розуміти, як саме штучний інтелект (ChatGPT, Perplexity, Gemini) бачить ваш бізнес, 
        і надаємо інструменти для покращення вашої видимості у відповідях AI.
        
        **Наша цінність:**
        * 🔍 **Прозорість:** Бачте те, що бачить AI.
        * 📊 **Вимірюваність:** Перетворіть абстрактні "згадки" на конкретні метрики (SOV, Rank, Sentiment).
        * 📈 **Вплив:** Отримуйте рекомендації, як потрапити у відповіді AI та рекомендації.
        """)
        st.info("📧 **Технічна підтримка:** support@virshi.ai")

    st.divider()
    st.subheader("Топ-20 найчастіших запитань")

    faq_data = [
        ("Що таке GEO (Generative Engine Optimization)?", "Це процес оптимізації контенту вашого бренду, щоб він частіше і якісніше з'являвся у відповідях генеративних моделей (LLM), таких як GPT-4, Gemini тощо."),
        ("Що таке Share of Voice (SOV)?", "Це метрика, яка показує частку згадок вашого бренду серед усіх брендів, знайдених у відповіді на конкретний запит."),
        ("Як визначається тональність?", "Наші алгоритми аналізують контекст згадки (прикметники, емоційне забарвлення) і класифікують її як Позитивну, Нейтральну або Негативну."),
        ("Що таке 'Офіційні джерела' (Whitelist)?", "Це список ваших підконтрольних доменів (сайт, соцмережі). Ми відстежуємо, чи посилається AI саме на них як на джерело істини."),
        ("Як часто оновлюються дані?", "Якщо увімкнено автосканування, дані оновлюються щодня або щотижня (залежно від налаштувань). Ручний запуск дає миттєвий результат."),
        ("Чому мій бренд не знайдено?", "Можливо, AI ще не проіндексував ваш контент, або ваш бренд має низьку авторитетність у темі запиту. Скористайтеся вкладкою 'Рекомендації'."),
        ("Чи можу я додати конкурентів?", "Так, система автоматично визначає конкурентів у відповідях. Ви також можете бачити їх у розділі 'Конкуренти'."),
        ("Які моделі (LLM) підтримуються?", "Наразі ми підтримуємо Perplexity, OpenAI GPT-4o та Google Gemini Pro."),
        ("Чим відрізняється Trial від Active?", "У Trial режимі ви можете сканувати лише обмежену кількість запитів і тільки через Gemini. Active знімає ці обмеження."),
        ("Як працює імпорт запитів?", "Ви можете завантажити Excel-файл або вставити посилання на Google Sheet. Перша колонка має називатися 'Keyword'."),
        ("Що робити, якщо статус проекту 'Blocked'?", "Зверніться до адміністратора або на пошту підтримки для вирішення питань з оплатою або доступом."),
        ("Чи впливає SEO сайту на GEO?", "Так, технічне SEO та якість контенту є фундаментом для того, щоб LLM взагалі могли 'прочитати' ваш сайт."),
        ("Як покращити позицію (Rank)?", "Структуруйте дані, використовуйте списки, чіткі відповіді на питання та збільшуйте кількість цитувань у авторитетних джерелах."),
        ("Чи можна експортувати звіти?", "Так, ви можете завантажувати дані у форматі Excel або генерувати HTML-звіти у вкладці 'Рекомендації'."),
        ("Що таке 'Присутність у запитах'?", "Це відсоток запитів, на які AI хоча б раз згадав ваш бренд (незалежно від позиції)."),
        ("Як змінити назву бренду?", "Назва бренду задається при створенні проекту. Для зміни зверніться до адміністратора."),
        ("Чи бачать інші користувачі мої дані?", "Ні, дані суворо розділені між проектами та користувачами."),
        ("Скільки запитів я можу додати?", "Ліміт залежить від вашого тарифного плану. У Trial версії ліміт зазвичай 10 запитів."),
        ("Що означає помилка 'Timeout'?", "Це означає, що LLM відповідала занадто довго. Спробуйте повторити запит пізніше."),
        ("Як видалити проект?", "Видалення проекту доступне через адміністратора. Надішліть запит на support@virshi.ai.")
    ]

    for question, answer in faq_data:
        with st.expander(f"🔹 {question}"):
            st.write(answer)

def generate_html_report_content(project_name, df_scans, df_mentions, df_sources):
    """
    Генерує HTML-звіт.
    ВИПРАВЛЕНО: Нормалізація ID (UUID -> str) для коректного розрахунку метрик.
    """
    import pandas as pd
    from datetime import datetime
    import numpy as np
    import re

    current_date = datetime.now().strftime('%d.%m.%Y')
    
    # ==========================================
    # 🔥 0. DATA NORMALIZATION (FIXING ZEROS)
    # ==========================================
    
    # 1. Приводимо всі ID до рядків і чистимо пробіли
    # Це гарантує, що UUID з бази і об'єкти Pandas будуть ідентичними
    df_scans['id'] = df_scans['id'].astype(str).str.strip()
    
    # 2. Обробка таблиці згадок
    if not df_mentions.empty:
        df_mentions['scan_result_id'] = df_mentions['scan_result_id'].astype(str).str.strip()
        
        # Конвертуємо числові поля (захист від помилок)
        df_mentions['mention_count'] = pd.to_numeric(df_mentions['mention_count'], errors='coerce').fillna(0)
        df_mentions['rank_position'] = pd.to_numeric(df_mentions['rank_position'], errors='coerce').fillna(0)
        
        # Нормалізація 'is_my_brand' (обробляє True, 'true', '1', 1)
        # Перетворюємо в string, потім в нижній регістр, перевіряємо входження
        df_mentions['is_my_brand'] = df_mentions['is_my_brand'].astype(str).str.lower().isin(['true', '1', 't', 'yes', 'on'])
    else:
        # Створюємо пустий DataFrame з потрібними колонками, щоб код не впав
        df_mentions = pd.DataFrame(columns=['scan_result_id', 'mention_count', 'rank_position', 'is_my_brand', 'sentiment_score', 'brand_name'])

    # 3. Обробка таблиці джерел
    if not df_sources.empty:
        df_sources['scan_result_id'] = df_sources['scan_result_id'].astype(str).str.strip()
        df_sources['is_official'] = df_sources['is_official'].astype(str).str.lower().isin(['true', '1', 't', 'yes', 'on'])
    else:
        df_sources = pd.DataFrame(columns=['scan_result_id', 'url', 'is_official'])

    # Helper format text
    def format_llm_text(text):
        if pd.isna(text) or not text: return "Текст відповіді відсутній."
        txt = str(text)
        txt = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', txt)
        txt = txt.replace('* ', '<br>• ')
        txt = txt.replace('\n', '<br>')
        return txt

    def safe_int(val):
        try: return int(float(val))
        except: return 0

    # Provider Mapping
    PROVIDER_MAPPING = {
        "perplexity": "Perplexity",
        "gpt-4o": "OpenAI GPT",
        "gpt-4": "OpenAI GPT",
        "gemini-1.5-pro": "Google Gemini",
        "gemini": "Google Gemini"
    }
    
    def get_pretty_name(p):
        p_str = str(p).lower()
        for k, v in PROVIDER_MAPPING.items():
            if k in p_str: return v
        return str(p).capitalize()

    df_scans['provider_ui'] = df_scans['provider'].apply(get_pretty_name)
    providers_ui = sorted(df_scans['provider_ui'].unique().tolist())

    # ---------------------------------------------------------
    # CSS
    # ---------------------------------------------------------
    css_styles = '''
    @font-face { font-family: 'Golca'; src: url('') format('woff2'); font-weight: normal; font-style: normal; }
    * { box-sizing: border-box; }
    body { margin: 0; padding: 20px; background-color: #00d18f; font-family: 'Golca', 'Montserrat', sans-serif; color: #333; line-height: 1.6; }
    .content-card { background: #ffffff; border-radius: 20px; padding: 40px; max-width: 1000px; margin: 0 auto; box-shadow: 0 10px 40px rgba(0,0,0,0.15); }
    .virshi-logo-container { text-align: center; margin: 0 auto 20px auto; }
    .logo-img-real { max-width: 150px; height: auto; }
    .report-header { text-align: center; border-bottom: 2px solid #eee; padding-bottom: 20px; margin-bottom: 30px; }
    h1 { font-size: 28px; color: #2c3e50; margin: 0; font-weight: 800; }
    .subtitle { color: #888; margin-top: 10px; font-size: 14px; }
    
    .tabs-nav { display: flex; justify-content: center; gap: 10px; margin-bottom: 30px; flex-wrap: wrap; }
    .tab-btn { padding: 12px 25px; border: 2px solid #00d18f; background: #fff; color: #00d18f; border-radius: 30px; cursor: pointer; font-weight: 800; font-size: 14px; transition: all 0.3s ease; text-transform: uppercase; }
    .tab-btn.active, .tab-btn:hover { background: #00d18f; color: #fff; }
    .tab-content { display: none; animation: fadeIn 0.5s; }
    .tab-content.active { display: block; }
    @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }

    .kpi-row { display: flex; flex-wrap: wrap; justify-content: space-between; gap: 15px; margin-bottom: 20px; }
    .kpi-box { flex: 1 1 220px; border: 2px solid #00d18f; border-radius: 15px; padding: 20px; text-align: center; background: #e0f2f1; display: flex; flex-direction: column; align-items: center; justify-content: flex-start; position: relative; min-height: 200px; }
    .kpi-title { font-size: 13px; text-transform: uppercase; font-weight: bold; color: #555; margin-bottom: 10px; height: 30px; display: flex; align-items: center; }
    .kpi-big-num { font-size: 28px; font-weight: 800; color: #2c3e50; margin-bottom: 10px; }
    .chart-container { position: relative; width: 130px; height: 130px; margin: auto; }
    .kpi-tooltip { visibility: hidden; opacity: 0; width: 220px; background-color: #2c3e50; color: #fff; text-align: center; border-radius: 8px; padding: 10px; position: absolute; z-index: 100; bottom: 105%; left: 50%; transform: translateX(-50%); font-size: 11px; transition: opacity 0.3s; pointer-events: none; }
    .kpi-box:hover .kpi-tooltip { visibility: visible; opacity: 1; }
    .custom-legend { display: flex; flex-wrap: wrap; justify-content: center; gap: 10px; margin-top: 10px; font-size: 11px; font-weight: bold; color: #555; }
    .legend-item { display: flex; align-items: center; }
    .legend-dot { width: 12px; height: 8px; margin-right: 5px; border-radius: 2px; display: inline-block; }

    h3 { font-size: 20px; color: #2c3e50; margin-top: 40px; margin-bottom: 20px; padding-left: 15px; border-left: 5px solid #00d18f; font-weight: 800; }

    .item-box { border: 2px solid #4DD0E1; border-radius: 15px; margin-bottom: 20px; overflow: hidden; background: #fff; }
    .accordion-trigger { background: #fff; padding: 15px 20px; display: flex; align-items: center; gap: 15px; cursor: pointer; transition: 0.3s; justify-content: space-between; }
    .accordion-trigger:hover { background-color: #f9f9f9; }
    .accordion-trigger.active { background-color: #f0fdff; border-bottom: 1px solid #eee; }
    .item-number-wrapper { width: 36px; height: 36px; background: #00d18f; border-radius: 50%; display: flex; align-items: center; justify-content: center; color: #fff; font-weight: bold; font-size: 14px; flex-shrink: 0; }
    .item-query { font-size: 15px; font-weight: bold; color: #333; flex-grow: 1; margin-left: 15px;}
    
    .cards-row { display: flex; flex-wrap: wrap; gap: 10px; padding: 20px; background: #fff; border-bottom: 1px solid #eee; }
    .metric-card { flex: 1 1 200px; background: #ffffff; border: 1px solid #e0e0e0; border-top: 4px solid #00d18f; border-radius: 8px; padding: 15px; text-align: center; }
    .mc-label { font-size: 10px; font-weight: 700; text-transform: uppercase; color: #888; margin-bottom: 5px; display:flex; align-items:center; justify-content:center; gap:5px; }
    .mc-val { font-size: 20px; font-weight: 800; color: #333; }
    .info-icon { display:inline-block; width:14px; height:14px; background:#3b82f6; color:white; border-radius:50%; font-size:10px; line-height:14px; text-align:center; cursor:help; }

    .item-response { background-color: #f9fafb; color: #1d192b; padding: 25px; font-size: 14px; text-align: left; line-height: 1.6; }
    .response-label { font-weight: bold; color: #5e35b1; margin-bottom: 15px; display: block; font-size: 16px; border-bottom: 1px dashed #5e35b1; padding-bottom: 5px; width: fit-content; }

    .detail-charts-wrapper { display: flex; flex-wrap: wrap; gap: 20px; margin-top: 30px; padding-top: 20px; border-top: 1px solid #e0e0e0; }
    .detail-chart-block { flex: 1 1 400px; min-width: 0; }
    .detail-title { font-weight: bold; font-size: 14px; margin-bottom: 10px; color: #2c3e50; border-left: 3px solid #00d18f; padding-left: 10px; }
    
    table.inner-table { width: 100%; border-collapse: collapse; font-size: 12px; border: 1px solid #eee; }
    table.inner-table th { background: #f1f3f5; padding: 8px; text-align: left; color: #555; font-weight: 600; border-bottom: 1px solid #ddd; }
    table.inner-table td { padding: 8px; border-bottom: 1px solid #eee; color: #333; }
    
    .cta-block { margin-top: 40px; padding: 20px; background-color: #e0f2f1; border: 2px solid #00d18f; border-radius: 15px; text-align: center; font-size: 12px; }
    @media (min-width: 768px) { .content-card { padding: 50px; } }
    '''

    # JS
    js_block = '''
    <script>
    Chart.defaults.font.family = "'Golca', 'Montserrat', sans-serif";
    Chart.defaults.plugins.tooltip.enabled = true;
    const colorMain = "#00d18f"; const colorOfficial = "#4DD0E1"; const colorEmpty = "#ffcdd2";

    function createDoughnut(id, val, activeColor) {
        var ctx = document.getElementById(id);
        if(!ctx) return;
        new Chart(ctx, {
            type: 'doughnut',
            data: { datasets: [{ data: [val, 100 - val], backgroundColor: [activeColor, colorEmpty], borderWidth: 0, hoverOffset: 4 }] },
            options: { layout: { padding: 10 }, responsive: true, maintainAspectRatio: false, cutout: '70%', plugins: { legend: { display: false }, tooltip: { enabled: false } } }
        });
    }
    function openTab(evt, tabName) {
        var i, tabcontent, tablinks;
        tabcontent = document.getElementsByClassName("tab-content");
        for (i = 0; i < tabcontent.length; i++) { tabcontent[i].style.display = "none"; }
        tablinks = document.getElementsByClassName("tab-btn");
        for (i = 0; i < tablinks.length; i++) { tablinks[i].className = tablinks[i].className.replace(" active", ""); }
        document.getElementById(tabName).style.display = "block";
        evt.currentTarget.className += " active";
    }
    function toggleAcc(el) {
        el.classList.toggle("active");
        var panel = el.nextElementSibling;
        if (panel.style.display === "block") { panel.style.display = "none"; } else { panel.style.display = "block"; }
    }
    window.addEventListener('load', function() { __JS_CHARTS_PLACEHOLDER__ });
    </script>
    '''

    html_template = '''<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Звіт AI Visibility</title>
<link rel="icon" type="image/png" href="https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/faviconV2.png">
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;700;800;900&display=swap" rel="stylesheet">
<style>__CSS_PLACEHOLDER__</style>
</head>
<body>
<div class="content-card">
    <div class="virshi-logo-container"><img src="https://raw.githubusercontent.com/virshi-ai/image/39ba460ec649893b9495427aa102420beb1fa48d/virshi-op_logo-main.png" class="logo-img-real" alt="VIRSHI Logo"></div>
    <div class="report-header"><h1>Звіт AI Visibility: __PROJECT_NAME__</h1><div class="subtitle">Дата формування: __DATE__</div></div>
    <div class="tabs-nav">__TABS_BUTTONS__</div>
    __TABS_CONTENT__
    <div class="cta-block"><p>Повний аудит Al Visibility.</p><p>Напишіть нам: <a href="mailto:hi@virshi.ai">hi@virshi.ai</a></p></div>
</div>
__JS_BLOCK__
</body>
</html>'''

    tabs_buttons_html = ""
    for i, prov in enumerate(providers_ui):
        active_cls = "active" if i == 0 else ""
        prov_id = str(prov).replace(" ", "_").replace(".", "")
        tabs_buttons_html += f'<button class="tab-btn {active_cls}" onclick="openTab(event, \'{prov_id}\')">{prov}</button>\n'

    tabs_content_html = ""
    js_charts_code = ""

    # TOOLTIPS
    tt_sov = "Частка видимості вашого бренду у відповідях ШІ порівняно з конкурентами."
    tt_off = "Частка посилань, які ведуть на ваші офіційні ресурси."
    tt_sent = "Тональність, у якій ШІ описує бренд."
    tt_pos = "Середня позиція вашого бренду у відповідях ШІ"
    tt_brand_cov = "Відсоток запитів, у яких бренд був згаданий хоча б один раз."
    tt_domain_cov = "Відсоток запитів, у яких ШІ надав клікабельне посилання на ваш домен."

    for i, prov_ui in enumerate(providers_ui):
        active_cls = "style='display:block;'" if i == 0 else "style='display:none;'"
        prov_id = str(prov_ui).replace(" ", "_").replace(".", "")
        
        # Filter Data
        df_p = df_scans[df_scans['provider_ui'] == prov_ui].copy()
        if df_p.empty: continue
        
        scan_ids_in_prov = df_p['id'].tolist()
        
        # Filter Details (By ID List)
        mentions_prov = df_mentions[df_mentions['scan_result_id'].isin(scan_ids_in_prov)].copy()
        sources_prov = df_sources[df_sources['scan_result_id'].isin(scan_ids_in_prov)].copy()
        
        total_queries = len(df_p)
        
        # --- GLOBAL MATH ---
        mentions_prov['mention_count'] = mentions_prov['mention_count'].fillna(0)
        
        total_market_mentions = mentions_prov['mention_count'].sum()
        my_total_mentions = mentions_prov[mentions_prov['is_my_brand'] == True]['mention_count'].sum()
        sov_pct = (my_total_mentions / total_market_mentions * 100) if total_market_mentions > 0 else 0
        
        total_links = len(sources_prov)
        official_links = len(sources_prov[sources_prov['is_official'] == True])
        off_pct = (official_links / total_links * 100) if total_links > 0 else 0
        
        scans_with_brand = mentions_prov[(mentions_prov['is_my_brand'] == True) & (mentions_prov['mention_count'] > 0)]['scan_result_id'].nunique()
        brand_cov_pct = (scans_with_brand / total_queries * 100) if total_queries > 0 else 0
        
        scans_with_off_link = sources_prov[sources_prov['is_official'] == True]['scan_result_id'].nunique()
        domain_cov_pct = (scans_with_off_link / total_queries * 100) if total_queries > 0 else 0
        
        avg_pos = 0
        if not mentions_prov.empty:
            my_ranks = mentions_prov[(mentions_prov['is_my_brand'] == True) & (mentions_prov['rank_position'] > 0)]['rank_position']
            avg_pos = my_ranks.mean() if not my_ranks.empty else 0
        
        sent_label = "Нейтральна"
        if not mentions_prov.empty:
            valid_sent = mentions_prov[(mentions_prov['is_my_brand'] == True) & (mentions_prov['sentiment_score'] != 'Не згадано')]
            if not valid_sent.empty:
                mode = valid_sent['sentiment_score'].mode()
                if not mode.empty: sent_label = mode[0]

        # --- HTML TAB ---
        tabs_content_html += f'''
        <div id="{prov_id}" class="tab-content" {active_cls}>
            <div class="kpi-row">
                <div class="kpi-box"><div class="kpi-tooltip">{tt_sov}</div><div class="kpi-title">Частка голосу (SOV)</div><div class="kpi-big-num">{sov_pct:.2f}%</div><div class="chart-container"><canvas id="chartSOV_{prov_id}"></canvas></div></div>
                <div class="kpi-box"><div class="kpi-tooltip">{tt_off}</div><div class="kpi-title">% Офіційних джерел</div><div class="kpi-big-num">{off_pct:.2f}%</div><div class="chart-container"><canvas id="chartOfficial_{prov_id}"></canvas></div></div>
                <div class="kpi-box"><div class="kpi-tooltip">{tt_sent}</div><div class="kpi-title">Загальна тональність</div><div class="kpi-big-num" style="font-size:20px;">{sent_label}</div><div class="chart-container"><canvas id="chartSentiment_{prov_id}"></canvas></div></div>
            </div>
            <div class="kpi-row">
                <div class="kpi-box"><div class="kpi-tooltip">{tt_pos}</div><div class="kpi-title">Позиція бренду</div><div class="kpi-big-num">{avg_pos:.1f}</div><div class="chart-container"><canvas id="chartPos_{prov_id}"></canvas></div></div>
                <div class="kpi-box"><div class="kpi-tooltip">{tt_brand_cov}</div><div class="kpi-title">Присутність бренду</div><div class="kpi-big-num">{brand_cov_pct:.1f}%</div><div class="chart-container"><canvas id="chartBrandCov_{prov_id}"></canvas></div></div>
                <div class="kpi-box"><div class="kpi-tooltip">{tt_domain_cov}</div><div class="kpi-title">Згадки домену</div><div class="kpi-big-num">{domain_cov_pct:.1f}%</div><div class="chart-container"><canvas id="chartDomainCov_{prov_id}"></canvas></div></div>
            </div>
            <h3 style="page-break-before: always;">Детальний аналіз запитів</h3>
            <div class="accordion-wrapper">
        '''

        # Loop Queries
        for idx, row in df_p.reset_index(drop=True).iterrows():
            q_text = row.get('keyword', 'Запит')
            scan_id = str(row['id']).strip() # ВАЖЛИВО: Очистка ID
            
            # --- LOCAL METRICS ---
            # Використовуємо .loc для точної фільтрації по строковому ID
            loc_mentions = mentions_prov[mentions_prov['scan_result_id'] == scan_id]
            loc_sources = sources_prov[sources_prov['scan_result_id'] == scan_id]
            
            # Local SOV
            l_tot = loc_mentions['mention_count'].sum()
            l_my_row = loc_mentions[loc_mentions['is_my_brand'] == True]
            l_my = l_my_row['mention_count'].sum()
            
            l_sov = (l_my / l_tot * 100) if l_tot > 0 else 0.0
            
            # Metrics for Card
            l_count = safe_int(l_my)
            l_sent = "Нейтральна"
            l_pos = "-"
            
            if not l_my_row.empty:
                l_sent = l_my_row['sentiment_score'].iloc[0]
                val = l_my_row[l_my_row['rank_position'] > 0]['rank_position'].min()
                if pd.notnull(val) and val > 0: l_pos = f"#{safe_int(val)}"

            # --- TABLES ---
            details_html = ""
            has_brands = not loc_mentions.empty
            has_sources = not loc_sources.empty
            
            if has_brands or has_sources:
                details_html += '<div class="detail-charts-wrapper">'
                
                if has_brands:
                    rows_b = ""
                    sort_b = loc_mentions.sort_values(['is_my_brand', 'mention_count'], ascending=[False, False])
                    for _, b in sort_b.iterrows():
                        bg = "style='background:#e6fffa; font-weight:bold;'" if b['is_my_brand'] else ""
                        rows_b += f"<tr {bg}><td>{b['brand_name']}</td><td>{safe_int(b['mention_count'])}</td><td>{b.get('sentiment_score','-')}</td><td>{safe_int(b.get('rank_position',0))}</td></tr>"
                    details_html += f'<div class="detail-chart-block"><div class="detail-title">Знайдені бренди</div><div class="table-responsive"><table class="inner-table"><thead><tr><th>Бренд</th><th>Кіл.</th><th>Настрій</th><th>Поз.</th></tr></thead><tbody>{rows_b}</tbody></table></div></div>'
                
                if has_sources:
                    rows_s = ""
                    for _, s in loc_sources.iterrows():
                        icon = "✅" if s['is_official'] else "🔗"
                        url = str(s['url'])
                        rows_s += f"<tr><td style='word-break:break-all;'><a href='{url}' target='_blank' style='color:#00d18f; text-decoration:none;'>{url}</a></td><td>{icon}</td></tr>"
                    details_html += f'<div class="detail-chart-block"><div class="detail-title">Цитовані джерела</div><div class="table-responsive"><table class="inner-table"><thead><tr><th>URL</th><th>Тип</th></tr></thead><tbody>{rows_s}</tbody></table></div></div>'
                
                details_html += '</div>'

            # Response
            raw_t = row.get('raw_response', '')
            fmt_t = format_llm_text(raw_t)

            tabs_content_html += f'''
            <div class="item-box">
                <div class="item-header accordion-trigger" onclick="toggleAcc(this)">
                    <div class="item-number-wrapper"><span class="item-number">{idx+1}</span></div>
                    <div class="item-query">{q_text}</div>
                    <div class="accordion-arrow">▼</div>
                </div>
                <div class="accordion-content" style="display: none;">
                    <div class="cards-row">
                        <div class="metric-card"><div class="mc-label">SOV <span class="info-icon" title="Частка">%</span></div><div class="mc-val">{l_sov:.1f}%</div></div>
                        <div class="metric-card"><div class="mc-label">ЗГАДОК <span class="info-icon" title="Кількість">#</span></div><div class="mc-val">{l_count}</div></div>
                        <div class="metric-card"><div class="mc-label">ТОНАЛЬНІСТЬ <span class="info-icon" title="Настрій">☺</span></div><div class="mc-val" style="font-size:18px;">{l_sent}</div></div>
                        <div class="metric-card"><div class="mc-label">ПОЗИЦІЯ <span class="info-icon" title="Ранг">1</span></div><div class="mc-val">{l_pos}</div></div>
                    </div>
                    <div class="item-response">
                        <div class="response-label">Відповідь LLM:</div>
                        {fmt_t}
                        {details_html}
                    </div>
                </div>
            </div>'''
        
        tabs_content_html += "</div></div>"

        # JS Charts Logic
        js_charts_code += f"createDoughnut('chartSOV_{prov_id}', {sov_pct}, '#00d18f');\n"
        js_charts_code += f"createDoughnut('chartOfficial_{prov_id}', {off_pct}, '#4DD0E1');\n"
        js_charts_code += f"createDoughnut('chartBrandCov_{prov_id}', {brand_cov_pct}, '#00d18f');\n"
        js_charts_code += f"createDoughnut('chartDomainCov_{prov_id}', {domain_cov_pct}, '#4DD0E1');\n"
        js_charts_code += f"createDoughnut('chartSentiment_{prov_id}', 100, '#adb5bd');\n"
        score_pos = max(0, 11 - avg_pos) if avg_pos > 0 else 0
        js_charts_code += f"createDoughnut('chartPos_{prov_id}', {score_pos * 10}, '#00d18f');\n"

    final_js = js_block.replace("__JS_CHARTS_PLACEHOLDER__", js_charts_code)
    final_html = html_template.replace("__CSS_PLACEHOLDER__", css_styles)\
        .replace("__PROJECT_NAME__", str(project_name))\
        .replace("__DATE__", str(current_date))\
        .replace("__TABS_BUTTONS__", tabs_buttons_html)\
        .replace("__TABS_CONTENT__", tabs_content_html)\
        .replace("__JS_BLOCK__", final_js)

    return final_html
    

def show_reports_page():
    """
    Сторінка Звітів.
    Збирає дані, приводить ID до рядків (str), передає в генератор.
    """
    import streamlit as st
    import pandas as pd
    from datetime import datetime
    
    st.title("📊 Звіти")

    if 'supabase' in st.session_state:
        supabase = st.session_state['supabase']
    elif 'supabase' in globals():
        supabase = globals()['supabase']
    else:
        st.error("🚨 Помилка підключення до БД.")
        return
    
    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Оберіть проект.")
        return

    user_role = st.session_state.get("role", "user")
    is_admin = (user_role in ["admin", "super_admin"])
    
    tabs = st.tabs(["📥 Замовити звіт", "📂 Готові звіти"] + (["⚙️ Адмінка"] if is_admin else []))

    # === ЗАМОВЛЕННЯ ===
    with tabs[0]:
        st.markdown("### Створення нового звіту")
        st.info("Звіт формується на основі останніх актуальних сканувань.")
        
        rep_name = st.text_input("Назва звіту", value=f"Звіт {proj.get('brand_name')} - {datetime.now().strftime('%d.%m.%Y')}")
        
        if st.button("🚀 Згенерувати звіт", type="primary"):
            with st.spinner("Аналіз даних та генерація HTML..."):
                try:
                    kw_resp = supabase.table("keywords").select("id, keyword_text").eq("project_id", proj["id"]).execute()
                    kw_map = {k['id']: k['keyword_text'] for k in kw_resp.data} if kw_resp.data else {}
                    if not kw_map:
                        st.error("Немає запитів.")
                        st.stop()

                    scans_resp = supabase.table("scan_results")\
                        .select("id, created_at, provider, keyword_id, raw_response")\
                        .eq("project_id", proj["id"])\
                        .order("created_at", desc=True)\
                        .limit(3000)\
                        .execute()
                    
                    raw_scans = scans_resp.data if scans_resp.data else []
                    if not raw_scans:
                        st.error("Історія пуста.")
                        st.stop()

                    df_raw = pd.DataFrame(raw_scans)
                    df_raw = df_raw.sort_values('created_at', ascending=False)
                    df_latest = df_raw.drop_duplicates(subset=['keyword_id', 'provider'], keep='first').copy()
                    
                    # Convert IDs to string to match correctly
                    df_latest['id'] = df_latest['id'].astype(str)
                    scan_ids = df_latest['id'].tolist()
                    
                    # Details
                    m_resp = supabase.table("brand_mentions").select("*").in_("scan_result_id", scan_ids).execute()
                    s_resp = supabase.table("extracted_sources").select("*").in_("scan_result_id", scan_ids).execute()
                    
                    mentions_df = pd.DataFrame(m_resp.data) if m_resp.data else pd.DataFrame()
                    sources_df = pd.DataFrame(s_resp.data) if s_resp.data else pd.DataFrame()

                    # Data Prep
                    df_latest['keyword'] = df_latest['keyword_id'].map(kw_map).fillna("Unknown")
                    try: df_latest['created_at_dt'] = pd.to_datetime(df_latest['created_at'])
                    except: pass
                    
                    # CLEANING & TYPE CASTING
                    if not mentions_df.empty:
                        mentions_df['scan_result_id'] = mentions_df['scan_result_id'].astype(str)
                    else:
                        mentions_df = pd.DataFrame(columns=['scan_result_id', 'brand_name', 'mention_count', 'is_my_brand'])

                    if not sources_df.empty:
                        sources_df['scan_result_id'] = sources_df['scan_result_id'].astype(str)
                    else:
                        sources_df = pd.DataFrame(columns=['scan_result_id', 'url', 'is_official'])

                    # Call Generator
                    html_code = generate_html_report_content(proj.get('brand_name'), df_latest, mentions_df, sources_df)

                    # Save
                    supabase.table("reports").insert({
                        "project_id": proj["id"],
                        "report_name": rep_name,
                        "html_content": html_code,
                        "status": "pending"
                    }).execute()
                    
                    st.success("✅ Звіт успішно сформовано! (Вкладка Адмінка)")
                    
                except Exception as e:
                    st.error(f"Помилка: {e}")

    # === ГОТОВІ ===
    with tabs[1]:
        st.markdown("### 📂 Архів")
        try:
            pub_resp = supabase.table("reports").select("*").eq("project_id", proj["id"]).eq("status", "published").order("created_at", desc=True).execute()
            reports = pub_resp.data if pub_resp.data else []
            
            if not reports:
                st.info("Немає опублікованих звітів.")
            else:
                for r in reports:
                    with st.expander(f"📄 {r['report_name']} ({r['created_at'][:10]})"):
                        c1, c2 = st.columns([1, 2])
                        with c1:
                            st.download_button("📥 Завантажити HTML", r['html_content'], file_name=f"{r['report_name']}.html", mime="text/html")
                        with c2:
                            if st.checkbox("Показати", key=f"sh_{r['id']}"):
                                st.components.v1.html(r['html_content'], height=800, scrolling=True)
        except Exception as e:
            st.error(f"Помилка: {e}")

    # === АДМІНКА ===
    if is_admin:
        with tabs[2]:
            st.markdown("### ⚙️ Модерація (Pending)")
            try:
                pend_resp = supabase.table("reports").select("*").eq("project_id", proj["id"]).eq("status", "pending").order("created_at", desc=True).execute()
                pending = pend_resp.data if pend_resp.data else []
                
                if not pending:
                    st.info("Черга пуста.")
                else:
                    for pr in pending:
                        st.divider()
                        st.subheader(f"📝 {pr['report_name']}")
                        new_html = st.text_area("Редактор HTML:", value=pr['html_content'], height=300, key=f"ed_{pr['id']}")
                        c1, c2 = st.columns([1, 4])
                        if c1.button("✅ Опублікувати", key=f"pub_{pr['id']}", type="primary"):
                            supabase.table("reports").update({"status": "published", "html_content": new_html}).eq("id", pr['id']).execute()
                            st.success("Опубліковано!"); st.rerun()
                        if c2.button("❌ Видалити", key=f"del_{pr['id']}"):
                            supabase.table("reports").delete().eq("id", pr['id']).execute()
                            st.rerun()
            except Exception as e:
                st.error(f"Помилка: {e}")
                

def show_dashboard():
    """
    Сторінка Дашборд.
    ВЕРСІЯ: FINAL UI ADJUSTMENTS.
    1. Огляд моделей: Додано заголовок "Тональність".
    2. Конкуренти: Прибрано LLM-стовпчики, SOV/Присутність цифрами.
    3. Деталізація: Червоний великий конкурент, метрики цільового бренду.
    """
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    import streamlit as st
    from datetime import datetime, timedelta

    # --- 1. ПІДКЛЮЧЕННЯ ---
    if 'supabase' in st.session_state:
        supabase = st.session_state['supabase']
    elif 'supabase' in globals():
        supabase = globals()['supabase']
    else:
        st.error("🚨 Помилка: Змінна 'supabase' не знайдена. Оновіть сторінку.")
        return

    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект.")
        return

    # --- CSS ---
    st.markdown("""
    <style>
        h3 { font-size: 1.15rem !important; font-weight: 600 !important; padding-top: 20px !important; }
        .green-number { background-color: #00C896; color: white; width: 24px; height: 24px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-weight: bold; font-size: 12px; }
        .comp-tag { background: #f3f4f6; padding: 2px 6px; border-radius: 4px; font-size: 11px; color: #555; }
        
        /* Стиль для блоку тональності в картках */
        .sent-container {
            display: flex;
            flex-direction: column;
            align-items: center; /* Центрування по горизонталі */
            margin-top: 10px;
        }
        .sent-header {
            font-size: 12px;
            color: #555;
            margin-bottom: 4px;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 4px;
        }
        .sent-box { 
            display: flex; 
            gap: 12px; 
            font-size: 13px; 
            font-weight: 500; 
            background: #f9fafb;
            padding: 6px 12px;
            border-radius: 6px;
        }
        .sent-item { display: flex; align-items: center; gap: 4px; }
        
        /* Стиль для конкурента в деталізації (Червоний і Великий) */
        .competitor-highlight {
            color: #FF4B4B; 
            font-size: 14px; /* Збільшений шрифт */
            font-weight: 700;
        }
    </style>
    """, unsafe_allow_html=True)

    st.title(f"📊 Дашборд: {proj.get('brand_name')}")

    # ==============================================================================
    # 2. ОТРИМАННЯ ДАНИХ
    # ==============================================================================
    with st.spinner("Аналіз даних..."):
        try:
            kw_resp = supabase.table("keywords").select("id, keyword_text").eq("project_id", proj["id"]).execute()
            keywords_df = pd.DataFrame(kw_resp.data) if kw_resp.data else pd.DataFrame()
            
            scan_resp = supabase.table("scan_results")\
                .select("id, provider, created_at, keyword_id")\
                .eq("project_id", proj["id"])\
                .order("created_at", desc=True)\
                .execute()
            scans_df = pd.DataFrame(scan_resp.data) if scan_resp.data else pd.DataFrame()
            
            mentions_df = pd.DataFrame()
            sources_df = pd.DataFrame()
            
            if not scans_df.empty:
                scan_ids = scans_df['id'].tolist()
                m_resp = supabase.table("brand_mentions").select("*").in_("scan_result_id", scan_ids).execute()
                if m_resp.data: mentions_df = pd.DataFrame(m_resp.data)
                
                s_resp = supabase.table("extracted_sources").select("*").in_("scan_result_id", scan_ids).execute()
                if s_resp.data: sources_df = pd.DataFrame(s_resp.data)

        except Exception as e:
            st.error(f"Помилка завантаження даних: {e}")
            return

    if scans_df.empty:
        st.info("Даних ще немає. Запустіть сканування.")
        return

    # ==============================================================================
    # 3. ОБРОБКА ДАНИХ
    # ==============================================================================
    def norm_provider(p):
        p = str(p).lower()
        if 'perplexity' in p: return 'Perplexity'
        if 'gpt' in p: return 'OpenAI GPT'
        if 'gemini' in p: return 'Google Gemini'
        return 'Other'

    scans_df['provider_ui'] = scans_df['provider'].apply(norm_provider)
    scans_df['created_at'] = pd.to_datetime(scans_df['created_at'])

    target_brand_raw = proj.get('brand_name', '').strip()
    target_brand_lower = target_brand_raw.lower()
    
    if not mentions_df.empty:
        mentions_df['mention_count'] = pd.to_numeric(mentions_df['mention_count'], errors='coerce').fillna(0)
        mentions_df['rank_position'] = pd.to_numeric(mentions_df['rank_position'], errors='coerce').fillna(0)
        
        df_full = pd.merge(mentions_df, scans_df, left_on='scan_result_id', right_on='id', suffixes=('_m', '_s'))
        
        df_full['is_target'] = df_full.apply(
            lambda x: x.get('is_my_brand', False) or (target_brand_lower in str(x.get('brand_name', '')).lower()), axis=1
        )
    else:
        df_full = pd.DataFrame()

    # ==============================================================================
    # 4. МЕТРИКИ ПО МОДЕЛЯХ (ВИПРАВЛЕНО ЗАГОЛОВОК)
    # ==============================================================================
    st.markdown("### 🌐 Огляд по моделях")
    
    def get_llm_stats(model_name):
        model_scans = scans_df[scans_df['provider_ui'] == model_name]
        if model_scans.empty: return 0, 0, (0,0,0)
        
        latest_scans = model_scans.sort_values('created_at', ascending=False).drop_duplicates('keyword_id')
        target_scan_ids = latest_scans['id'].tolist()
        
        if not target_scan_ids or df_full.empty: return 0, 0, (0,0,0)

        current_mentions = df_full[df_full['scan_result_id'].isin(target_scan_ids)]
        if current_mentions.empty: return 0, 0, (0,0,0)

        total_mentions = current_mentions['mention_count'].sum()
        my_mentions = current_mentions[current_mentions['is_target'] == True]
        my_count = my_mentions['mention_count'].sum()
        
        sov = (my_count / total_mentions * 100) if total_mentions > 0 else 0
        
        valid_ranks = my_mentions[my_mentions['rank_position'] > 0]
        rank = valid_ranks['rank_position'].mean() if not valid_ranks.empty else 0
        
        # Тональність %
        pos, neu, neg = 0, 0, 0
        if not my_mentions.empty:
            total_sent = len(my_mentions)
            pos_c = len(my_mentions[my_mentions['sentiment_score'] == 'Позитивний'])
            neu_c = len(my_mentions[my_mentions['sentiment_score'] == 'Нейтральний'])
            neg_c = len(my_mentions[my_mentions['sentiment_score'] == 'Негативний'])
            
            if total_sent > 0:
                pos = int(pos_c / total_sent * 100)
                neu = int(neu_c / total_sent * 100)
                neg = int(neg_c / total_sent * 100)
                if pos + neu + neg < 100: neu += (100 - (pos+neu+neg))
            
        return sov, rank, (pos, neu, neg)

    cols = st.columns(3)
    models = ['Perplexity', 'OpenAI GPT', 'Google Gemini']
    
    for i, model in enumerate(models):
        with cols[i]:
            sov, rank, (pos, neu, neg) = get_llm_stats(model)
            with st.container(border=True):
                st.markdown(f"**{model}**")
                c1, c2 = st.columns(2)
                
                c1.metric("SOV", f"{sov:.1f}%", help="Share of Voice: Відсоток згадок вашого бренду.")
                c2.metric("Rank", f"#{rank:.1f}" if rank > 0 else "-", help="Середня позиція вашого бренду.")
                
                # Sentiment Box (Centered with Header)
                if pos == 0 and neu == 0 and neg == 0:
                    sent_html = """
                    <div class="sent-container">
                        <div class="sent-header">Тональність <span title="Немає даних">ℹ️</span></div>
                        <div style='color:#ccc; font-size:13px;'>Не знайдено</div>
                    </div>
                    """
                else:
                    sent_html = f"""
                    <div class="sent-container">
                        <div class="sent-header">
                            Тональність 
                            <span title="Розподіл: Позитив / Нейтрал / Негатив" style="cursor:help;">ℹ️</span>
                        </div>
                        <div class="sent-box">
                            <div class="sent-item" style="color:#00C896" title="Позитив">😄 {pos}%</div>
                            <div class="sent-item" style="color:#FFCE56" title="Нейтрал">😐 {neu}%</div>
                            <div class="sent-item" style="color:#FF4B4B" title="Негатив">😡 {neg}%</div>
                        </div>
                    </div>
                    """
                st.markdown(sent_html, unsafe_allow_html=True)

    # ==============================================================================
    # 5. ГРАФІК ДИНАМІКИ
    # ==============================================================================
    st.write("")
    st.markdown("### 📈 Динаміка бренду (SOV)")
    st.caption("Графік відображає зміну видимості вашого бренду в часі.")
    
    if not df_full.empty:
        df_full['date_day'] = df_full['created_at'].dt.floor('D')
        daily = df_full.groupby(['date_day', 'provider_ui']).apply(
            lambda x: pd.Series({
                'total': x['mention_count'].sum(),
                'my': x[x['is_target'] == True]['mention_count'].sum()
            })
        ).reset_index()
        daily['sov'] = (daily['my'] / daily['total'] * 100).fillna(0)
        
        fig = px.line(daily, x='date_day', y='sov', color='provider_ui', markers=True, 
                      color_discrete_map={'Perplexity':'#00C896', 'OpenAI GPT':'#FF4B4B', 'Google Gemini':'#3B82F6'})
        fig.update_layout(height=300, margin=dict(l=0,r=0,t=10,b=0), hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Немає даних.")

    # ==============================================================================
    # 6. КОНКУРЕНТНИЙ АНАЛІЗ (СПРОЩЕНИЙ ВИГЛЯД)
    # ==============================================================================
    st.write("")
    st.markdown("### 🏆 Конкурентний аналіз")
    st.caption("Зведена статистика по всіх моделях. Показники відображаються числами.")

    if not df_full.empty:
        total_mentions_all = df_full['mention_count'].sum()
        total_kws_all = df_full['keyword_id'].nunique()

        df_target_raw = df_full[df_full['is_target'] == True]
        df_competitors_raw = df_full[df_full['is_target'] == False]

        def get_dominant_sentiment(series):
            if series.empty: return "-"
            mode = series.mode()
            return mode[0] if not mode.empty else "Нейтральний"

        # 1. Наш бренд
        if not df_target_raw.empty:
            merged_target = pd.Series({
                'brand_name': f"🟢 {target_brand_raw} (Ви)",
                'mentions': df_target_raw['mention_count'].sum(),
                'unique_kws': df_target_raw['keyword_id'].nunique(),
                'sentiment': get_dominant_sentiment(df_target_raw['sentiment_score']),
                'first_seen': df_target_raw['created_at'].min(),
                'is_target': True
            })
            target_df = pd.DataFrame([merged_target])
        else:
            target_df = pd.DataFrame([{
                'brand_name': f"🟢 {target_brand_raw} (Ви)",
                'mentions': 0, 'unique_kws': 0, 'sentiment': '-', 'first_seen': None, 'is_target': True
            }])

        # 2. Конкуренти
        def agg_competitors(x):
            return pd.Series({
                'mentions': x['mention_count'].sum(),
                'unique_kws': x['keyword_id'].nunique(),
                'sentiment': get_dominant_sentiment(x['sentiment_score']),
                'first_seen': x['created_at'].min(),
                'is_target': False
            })
        
        if not df_competitors_raw.empty:
            competitors_agg = df_competitors_raw.groupby('brand_name').apply(agg_competitors).reset_index()
            competitors_top9 = competitors_agg.sort_values('mentions', ascending=False).head(9)
        else:
            competitors_top9 = pd.DataFrame()

        final_df = pd.concat([target_df, competitors_top9])
        final_df = final_df.sort_values('mentions', ascending=False)

        final_df['sov'] = (final_df['mentions'] / total_mentions_all).fillna(0)
        final_df['presence'] = (final_df['unique_kws'] / total_kws_all).fillna(0)

        rows = []
        for _, r in final_df.iterrows():
            d_str = r['first_seen'].strftime("%d.%m.%Y") if pd.notnull(r['first_seen']) else "-"
            rows.append({
                "Бренд": r['brand_name'], 
                "Згадок": r['mentions'],
                "SOV": r['sov'],
                "Присутність": r['presence'],
                "Тональність": r['sentiment'], 
                "Перша згадка": d_str
            })
            
        st.dataframe(
            pd.DataFrame(rows), 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "Згадок": st.column_config.NumberColumn(format="%d", help="Сумарна кількість згадок."),
                # ВАЖЛИВО: NumberColumn замість ProgressColumn
                "SOV": st.column_config.NumberColumn("Частка голосу (SOV)", format="%.1f%%", help="% від усіх згадок."),
                "Присутність": st.column_config.NumberColumn("Присутність", format="%.0f%%", help="% запитів, де знайдено бренд."),
                "Тональність": st.column_config.TextColumn("Тональність", help="Домінуюча тональність."),
            }
        )
    else:
        st.info("Немає даних для аналізу конкурентів.")

    # ==============================================================================
    # 7. ДЕТАЛЬНА СТАТИСТИКА
    # ==============================================================================
    st.write("")
    st.markdown("### 📋 Детальна статистика по запитах")
    st.caption("Метрики розраховані для вашого цільового бренду.")
    
    cols = st.columns([0.4, 2.5, 1, 1, 1, 1.2, 2])
    cols[1].markdown("**Запит**")
    cols[2].markdown("**Згадок**", help="Кількість згадок вашого бренду в цьому запиті.")
    cols[3].markdown("**SOV**", help="Ваша частка голосу в цьому запиті.")
    cols[4].markdown("**Позиція**", help="Середня позиція вашого бренду.")
    cols[5].markdown("**Тональність**", help="Переважаюча тональність вашого бренду.")
    cols[6].markdown("**Топ Конкурент / Джерела**")
    
    st.markdown("---")

    unique_kws = keywords_df.to_dict('records')
    
    for idx, kw in enumerate(unique_kws, 1):
        kw_id = kw['id']
        kw_text = kw['keyword_text']
        
        cur_sov, cur_rank, my_mentions_count = 0, 0, 0
        cur_sent = "—"
        top_comp_name, top_comp_val = "—", 0
        off_sources_count = 0
        has_data = False

        if not df_full.empty:
            kw_data = df_full[df_full['keyword_id'] == kw_id]
            
            if not kw_data.empty:
                has_data = True
                sorted_scans = kw_data.sort_values('created_at', ascending=False)
                latest_date = sorted_scans['created_at'].max()
                current_slice = sorted_scans[sorted_scans['created_at'] >= (latest_date - timedelta(hours=12))]

                # Наш бренд
                my_rows = current_slice[current_slice['is_target'] == True]
                my_mentions_count = my_rows['mention_count'].sum()
                tot = current_slice['mention_count'].sum()
                cur_sov = (my_mentions_count / tot * 100) if tot > 0 else 0
                
                ranks = my_rows[my_rows['rank_position'] > 0]['rank_position']
                cur_rank = ranks.mean() if not ranks.empty else 0
                
                cur_sent = my_rows['sentiment_score'].mode()[0] if not my_rows['sentiment_score'].mode().empty else "—"

                # Конкурент
                competitors = current_slice[current_slice['is_target'] == False]
                if not competitors.empty:
                    top_comp_name = competitors.groupby('brand_name')['mention_count'].sum().idxmax()
                    top_comp_val = competitors.groupby('brand_name')['mention_count'].sum().max()
                else:
                    top_comp_name = "Немає"; top_comp_val = 0
                    
                # Джерела
                if not sources_df.empty:
                    scan_ids_kw = current_slice['scan_result_id'].unique()
                    kw_sources = sources_df[sources_df['scan_result_id'].isin(scan_ids_kw)]
                    if 'is_official' in kw_sources.columns:
                        off_sources_count = len(kw_sources[kw_sources['is_official'] == True])

        with st.container():
            c = st.columns([0.4, 2.5, 1, 1, 1, 1.2, 2])
            c[0].markdown(f"<div class='green-number'>{idx}</div>", unsafe_allow_html=True)
            c[1].markdown(f"<span class='kw-row-text'>{kw_text}</span>", unsafe_allow_html=True)
            
            if has_data:
                c[2].markdown(f"**{int(my_mentions_count)}**", unsafe_allow_html=True)
                c[3].markdown(f"{cur_sov:.1f}%", unsafe_allow_html=True)
                c[4].markdown(f"#{cur_rank:.1f}" if cur_rank > 0 else "-", unsafe_allow_html=True)
                
                st_col = "#333"
                if "Поз" in str(cur_sent): st_col = "#00C896"
                elif "Нег" in str(cur_sent): st_col = "#FF4B4B"
                elif "Ней" in str(cur_sent): st_col = "#FFCE56"
                elif "—" in str(cur_sent): st_col = "#ccc"
                
                c[5].markdown(f"<span style='color:{st_col}; font-weight:bold'>{cur_sent}</span>", unsafe_allow_html=True)
                
                # Червоний і великий конкурент
                c[6].markdown(f"""
                <span class='competitor-highlight' title='Головний конкурент'>VS {top_comp_name} ({top_comp_val})</span><br>
                <span class='source-tag' title='Знайдено офіційних посилань'>🔗 Офіц: {off_sources_count}</span>
                """, unsafe_allow_html=True)
            else:
                for i in range(2, 7): c[i].caption("—")
        
        st.markdown("<hr style='margin: 5px 0; border-top: 1px solid #eee;'>", unsafe_allow_html=True)


        
# =========================
# 7. КЕРУВАННЯ ЗАПИТАМИ
# =========================

def show_keyword_details(kw_id):
    """
    Сторінка детальної аналітики одного запиту.
    ВЕРСІЯ: AUTO-REFRESH (ST.FRAGMENT).
    1. Дані сканувань, графіки та таби оновлюються автоматично кожні 5 сек.
    2. Виправлено відсутній імпорт uuid.
    """
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    import streamlit as st
    from datetime import datetime, timedelta
    import numpy as np
    import time
    import re
    import uuid # 🔥 ДОДАНО: Потрібен для генерації унікальних ключів графіків
    
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
    MODEL_CONFIG = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }
    ALL_MODELS_UI = list(MODEL_CONFIG.keys())
    
    # Функція нормалізації назв з бази
    def get_ui_model_name(db_name):
        for ui, db in MODEL_CONFIG.items():
            if db == db_name: return ui
        lower = str(db_name).lower()
        if "perplexity" in lower: return "Perplexity"
        if "gpt" in lower or "openai" in lower: return "OpenAI GPT"
        if "gemini" in lower or "google" in lower: return "Google Gemini"
        return db_name 

    def tooltip(text):
        return f'<span title="{text}" style="cursor:help; font-size:14px; color:#333; margin-left:4px;">ℹ️</span>'

    def normalize_url(u):
        u = str(u).strip()
        u = re.split(r'[)\]]', u)[0] 
        if not u.startswith(('http://', 'https://')): return f"https://{u}"
        return u

    # 1. ОТРИМАННЯ ДАНИХ ЗАПИТУ (СТАТИЧНА ЧАСТИНА)
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

    # ⚙️ БЛОК НАЛАШТУВАНЬ (ЗАЛИШАЄМО СТАТИЧНИМ, ЩОБ НЕ ЗБИВАТИ ВВІД)
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

    # =================================================================================
    # 🔥 LIVE SECTION: АВТО-ОНОВЛЕННЯ ДАНИХ (KPI, Charts, Tabs)
    # =================================================================================
    @st.fragment(run_every=5)
    def render_live_analytics():
        # 2. ОТРИМАННЯ ДАНИХ (Всередині фрагмента)
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
                
                # --- TIMEZONE FIX ---
                df_scans['created_at'] = pd.to_datetime(df_scans['created_at'])
                if df_scans['created_at'].dt.tz is None:
                    df_scans['created_at'] = df_scans['created_at'].dt.tz_localize('UTC')
                df_scans['created_at'] = df_scans['created_at'].dt.tz_convert('Europe/Kiev')
                df_scans['date_str'] = df_scans['created_at'].dt.strftime('%Y-%m-%d %H:%M')
                
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

            # SMART MERGE
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
                            key=f"sel_date_{ui_model_name}" 
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

                # --- БРЕНДИ ---
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
                            st.plotly_chart(
                                fig_brands,
                                use_container_width=True,
                                config={'displayModeBar': False},
                                key=f"brands_chart_{selected_scan_id}_{str(uuid.uuid4())[:8]}" # унікальний ключ
                            )
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

                # --- ДЖЕРЕЛА ---
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
                                st.plotly_chart(
                                    fig_src, 
                                    use_container_width=True, 
                                    config={'displayModeBar': False},
                                    key=f"src_chart_{selected_scan_id}_{str(uuid.uuid4())[:8]}"
                                )

                            with c_src_table:
                                st.dataframe(
                                    df_grouped_src[['url', 'count']],
                                    use_container_width=True, hide_index=True,
                                    column_config={
                                        "url": st.column_config.LinkColumn("Посилання", width="large", validate="^https?://"),
                                        "count": st.column_config.NumberColumn("К-сть", width="small")
                                    }
                                )
                        else:
                            st.info("URL не знайдено.")
                    else:
                        st.info("ℹ️ Джерел не знайдено.")
                except Exception as e:
                    st.error(f"Помилка завантаження джерел: {e}")

    # 🔥 ВИКЛИК LIVE ФРАГМЕНТА
    render_live_analytics()



def show_keywords_page():
    """
    Сторінка списку запитів.
    ВЕРСІЯ: FORCE UI UPDATE (DYNAMIC KEYS).
    1. Використання динамічних ключів для віджетів (bulk_update_counter).
    2. Це гарантує повне оновлення візуального стану чекбоксів при масових діях.
    3. Виправлено проблему 'залипання' старих значень.
    """
    import pandas as pd
    import streamlit as st
    from datetime import datetime
    import time
    import io 
    import re 
    
    # Ініціалізація лічильника для примусового оновлення UI
    if "bulk_update_counter" not in st.session_state:
        st.session_state["bulk_update_counter"] = 0

    # CSS Стилізація
    st.markdown("""
    <style>
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
        div[data-testid="stColumn"]:nth-of-type(3) button[kind="secondary"] {
            border: none;
            background: transparent;
            text-align: left;
            padding-left: 0;
            font-weight: 600;
            color: #31333F;
            box-shadow: none;
        }
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
    </style>
    """, unsafe_allow_html=True)

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

    # --- СИНХРОНІЗАЦІЯ З БД ---
    if "current_project" in st.session_state and st.session_state["current_project"]:
        try:
            curr_id = st.session_state["current_project"]["id"]
            refresh_resp = supabase.table("projects").select("*").eq("id", curr_id).execute()
            if refresh_resp.data:
                st.session_state["current_project"] = refresh_resp.data[0]
        except Exception:
            pass 

    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект в онбордингу.")
        return

    if st.session_state.get("focus_keyword_id"):
        if 'show_keyword_details' in globals():
            show_keyword_details(st.session_state["focus_keyword_id"])
            return

    st.markdown("<h3 style='padding-top:0;'>📋 Перелік запитів</h3>", unsafe_allow_html=True)

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
    # 2. БЛОК РЕДАГУВАННЯ
    # ========================================================
    with st.expander("✏️ Редагування запитів", expanded=False): 
        
        tab_manual, tab_import, tab_export, tab_auto = st.tabs(["✍️ Ввести вручну", "📥 Імпорт (Excel / URL)", "📤 Експорт (Excel)", "⚙️ Автозапуск"])

        # --- TAB 1: ВРУЧНУ ---
        with tab_manual:
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
                selected_models_manual = st.multiselect("LLM для першого скану:", list(MODEL_MAPPING.keys()), default=["Perplexity"], key="manual_multiselect")
            
            with c_submit:
                st.write("")
                st.write("")
                if st.button("🚀 Додати", use_container_width=True, type="primary", key="btn_add_manual"):
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
                                with st.spinner(f"Зберігаємо та запускаємо аналіз..."):
                                    if 'n8n_trigger_analysis' in globals():
                                        for new_kw in new_keywords_list:
                                            n8n_trigger_analysis(proj["id"], [new_kw], proj.get("brand_name"), models=selected_models_manual)
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

        # --- TAB 2: ІМПОРТ EXCEL / URL ---
        with tab_import:
            st.info("💡 Завантажте файл .xlsx або вставте посилання на Google Sheet. **Важливо:** Для Google Sheet має бути відкрито доступ (Anyone with the link). Перша колонка має називатися **Keyword**.")
            
            import_source = st.radio("Джерело:", ["Файл (.xlsx)", "Посилання (URL)"], horizontal=True)
            df_upload = None
            
            if import_source == "Файл (.xlsx)":
                uploaded_file = st.file_uploader("Оберіть файл Excel", type=["xlsx"])
                if uploaded_file:
                    try:
                        df_upload = pd.read_excel(uploaded_file)
                    except ImportError:
                        st.error("🚨 Відсутня бібліотека `openpyxl`. Будь ласка, додайте `openpyxl` у requirements.txt вашого проекту.")
                    except Exception as e:
                        st.error(f"Не вдалося прочитати файл: {e}")
            else: # URL
                import_url = st.text_input("Вставте посилання (Google Sheets або CSV):")
                if import_url:
                    try:
                        if "docs.google.com" in import_url:
                            match = re.search(r'/d/([a-zA-Z0-9-_]+)', import_url)
                            if match:
                                sheet_id = match.group(1)
                                csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
                                df_upload = pd.read_csv(csv_url)
                            else:
                                st.error("Не вдалося розпізнати ID Google таблиці. Перевірте посилання.")
                        elif import_url.endswith(".csv"):
                            df_upload = pd.read_csv(import_url)
                        elif import_url.endswith(".xlsx"):
                            df_upload = pd.read_excel(import_url)
                        else:
                            st.warning("Спробуємо прочитати як CSV...")
                            df_upload = pd.read_csv(import_url)
                    except Exception as e:
                        if "400" in str(e) or "403" in str(e):
                            st.error("🔒 Помилка доступу (HTTP 400/403).")
                        else:
                            st.error(f"Не вдалося завантажити: {e}")

            if df_upload is not None:
                target_col = None
                cols_lower = [str(c).lower().strip() for c in df_upload.columns]
                
                if "keyword" in cols_lower:
                    target_col = df_upload.columns[cols_lower.index("keyword")]
                elif "запит" in cols_lower:
                    target_col = df_upload.columns[cols_lower.index("запит")]
                else:
                    target_col = df_upload.columns[0] 
                
                preview_kws = df_upload[target_col].dropna().astype(str).tolist()
                st.write(f"✅ Знайдено **{len(preview_kws)}** запитів. Приклад: {preview_kws[:3]}")
                
                st.write("---")
                st.write("Оберіть дію:")
                
                c_imp_models, c_imp_btn1, c_imp_btn2 = st.columns([2, 1.5, 1.5])
                
                with c_imp_models:
                    selected_models_import = st.multiselect("LLM (тільки для кнопки аналізу):", list(MODEL_MAPPING.keys()), default=["Perplexity"], key="import_multiselect")
                
                with c_imp_btn1:
                    st.write("")
                    st.write("")
                    if st.button("📥 Тільки зберегти", use_container_width=True):
                        if preview_kws:
                            try:
                                insert_data = [{
                                    "project_id": proj["id"], "keyword_text": kw, "is_active": True, 
                                    "is_auto_scan": False, "frequency": "daily"
                                } for kw in preview_kws]
                                
                                supabase.table("keywords").insert(insert_data).execute()
                                st.success(f"Успішно збережено {len(preview_kws)} запитів!")
                                time.sleep(1.5)
                                st.rerun()
                            except Exception as e:
                                st.error(f"Помилка збереження: {e}")

                with c_imp_btn2:
                    st.write("")
                    st.write("")
                    if st.button("🚀 Зберегти та Аналізувати", type="primary", use_container_width=True):
                        if preview_kws:
                            try:
                                insert_data = [{
                                    "project_id": proj["id"], "keyword_text": kw, "is_active": True, 
                                    "is_auto_scan": False, "frequency": "daily"
                                } for kw in preview_kws]
                                
                                res = supabase.table("keywords").insert(insert_data).execute()
                                if res.data:
                                    with st.spinner(f"Обробка {len(preview_kws)} запитів..."):
                                        if 'n8n_trigger_analysis' in globals():
                                            my_bar = st.progress(0, text="Запуск...")
                                            total = len(preview_kws)
                                            for i, kw in enumerate(preview_kws):
                                                n8n_trigger_analysis(proj["id"], [kw], proj.get("brand_name"), models=selected_models_import)
                                                my_bar.progress((i + 1) / total)
                                                time.sleep(0.3)
                                        st.success("Успішно збережено та запущено!")
                                        time.sleep(2)
                                        st.rerun()
                            except Exception as e:
                                st.error(f"Помилка процесу: {e}")

        # --- TAB 3: ЕКСПОРТ EXCEL ---
        with tab_export:
            st.write("Натисніть кнопку нижче, щоб завантажити всі запити цього проекту в Excel.")
            try:
                kws_resp = supabase.table("keywords").select("id, keyword_text, created_at").eq("project_id", proj["id"]).execute()
                if kws_resp.data:
                    df_export = pd.DataFrame(kws_resp.data)
                    scan_resp = supabase.table("scan_results").select("keyword_id, created_at").eq("project_id", proj["id"]).order("created_at", desc=True).execute()
                    
                    last_scan_map = {}
                    if scan_resp.data:
                        for s in scan_resp.data:
                            if s['keyword_id'] not in last_scan_map:
                                last_scan_map[s['keyword_id']] = s['created_at']
                    
                    df_export['last_scan_date'] = df_export['id'].map(lambda x: last_scan_map.get(x, "-"))
                    df_export['created_at'] = pd.to_datetime(df_export['created_at']).dt.strftime('%Y-%m-%d %H:%M')
                    df_export['last_scan_date'] = df_export['last_scan_date'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d %H:%M') if x != "-" else "-")
                    
                    df_final = df_export[["keyword_text", "created_at", "last_scan_date"]].rename(columns={"keyword_text": "Keyword", "created_at": "Date Added", "last_scan_date": "Last Scan Date"})
                    
                    buffer = io.BytesIO()
                    try:
                        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                            df_final.to_excel(writer, index=False, sheet_name='Keywords')
                    except:
                         try:
                             with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                                 df_final.to_excel(writer, index=False, sheet_name='Keywords')
                         except ImportError:
                             st.error("Для експорту потрібна бібліотека `xlsxwriter` або `openpyxl`.")
                             buffer = None

                    if buffer:
                        st.download_button(label="📥 Завантажити Excel", data=buffer.getvalue(), file_name=f"keywords_{proj.get('brand_name')}.xlsx", mime="application/vnd.ms-excel", type="primary")
                else:
                    st.warning("У проекті ще немає запитів для експорту.")
            except Exception as e:
                st.error(f"Помилка підготовки експорту: {e}")

        # --- TAB 4: АВТОЗАПУСК (МАСОВЕ НАЛАШТУВАННЯ) ---
        with tab_auto:
            st.markdown("##### ⚙️ Масове налаштування автозапуску")
            
            allow_cron_global = proj.get('allow_cron', False)
            if not allow_cron_global:
                st.error("🔒 Автозапуск недоступний для цього проекту. Зверніться до адміністратора.")
            else:
                st.info("Тут ви можете керувати автоскануванням для **всіх** запитів одночасно.")

                c_freq, c_btn = st.columns([2, 1.5])
                
                with c_freq:
                    freq_map = {"Щодня": "daily", "Щотижня": "weekly", "Щомісяця": "monthly"}
                    selected_freq_ui = st.selectbox("Оберіть частоту для всіх запитів:", list(freq_map.keys()))
                    selected_freq_db = freq_map[selected_freq_ui]

                with c_btn:
                    st.write("") # spacer
                    st.write("")
                    
                    # КНОПКА: УВІМКНУТИ ВСЕ
                    if st.button("✅ Застосувати частоту та Увімкнути", type="primary", use_container_width=True):
                        try:
                            supabase.table("keywords").update({
                                "is_auto_scan": True,
                                "frequency": selected_freq_db
                            }).eq("project_id", proj["id"]).execute()
                            
                            # 🔥 ГОЛОВНИЙ ФІКС: Змінюємо суфікс, щоб оновити ключі віджетів
                            st.session_state["bulk_update_counter"] += 1
                            
                            st.success(f"Оновлено! Всі запити будуть скануватися: {selected_freq_ui}")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Помилка оновлення: {e}")

                # КНОПКА: ВИМКНУТИ ВСЕ
                if st.button("⛔ Вимкнути автосканування для всіх", use_container_width=True):
                     try:
                        supabase.table("keywords").update({
                            "is_auto_scan": False
                        }).eq("project_id", proj["id"]).execute()

                        # 🔥 ГОЛОВНИЙ ФІКС: Змінюємо суфікс
                        st.session_state["bulk_update_counter"] += 1
                        
                        st.warning("Автосканування вимкнено для всіх запитів.")
                        time.sleep(1)
                        st.rerun()
                     except Exception as e:
                        st.error(f"Помилка: {e}")
                
                st.markdown("---")
                st.markdown("""
                **ℹ️ Як це працює:**
                1. **✅ Застосувати:** Активує автозапуск (`ON`) і встановлює обрану частоту для **всіх** запитів. Чекбокси внизу перемкнуться автоматично.
                2. **⛔ Вимкнути всі:** Деактивує автозапуск (`OFF`) для всіх запитів. Чекбокси внизу вимкнуться.
                3. **Синхронізація:** Стан перемикачів завжди відповідає даним у базі.
                """)

    st.divider()
    
    # ========================================================
    # 3. ОТРИМАННЯ ДАНИХ (ДЛЯ ТАБЛИЦІ НИЖЧЕ)
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
    # 4. & 5. ПАНЕЛЬ ТА СПИСОК (STABLE STATE FIX)
    # ========================================================

    update_suffix = st.session_state.get("bulk_update_counter", 0)

    # Функція-фрагмент (оновлюється незалежно)
    @st.fragment(run_every=5)
    def render_live_dashboard(keywords_data, proj_data, suffix_val):
        
        # --- 1. LIVE DATA FETCH ---
        # Отримуємо свіжі статуси сканування без перезавантаження всієї сторінки
        try:
            fresh_scans = supabase.table("scan_results").select("keyword_id, created_at").eq("project_id", proj_data["id"]).order("created_at", desc=True).execute()
            fresh_map = {}
            if fresh_scans.data:
                for s in fresh_scans.data:
                    if s['keyword_id'] not in fresh_map:
                        fresh_map[s['keyword_id']] = s['created_at']
            
            # Оновлюємо дати локально
            for k in keywords_data:
                k['last_scan_date'] = fresh_map.get(k['id'], "1970-01-01T00:00:00+00:00")
        except Exception:
            pass

        # --- 2. SORTING & FILTERING ---
        c_sort, _ = st.columns([2, 4])
        with c_sort:
            sort_option = st.selectbox("Сортувати за:", 
                                     ["Найновіші (Додані)", "Найстаріші (Додані)", "Нещодавно проскановані", "Давно не скановані"], 
                                     label_visibility="collapsed")

        sorted_kws = keywords_data.copy()
        if sort_option == "Найновіші (Додані)": sorted_kws.sort(key=lambda x: x['created_at'], reverse=True)
        elif sort_option == "Найстаріші (Додані)": sorted_kws.sort(key=lambda x: x['created_at'], reverse=False)
        elif sort_option == "Нещодавно проскановані": sorted_kws.sort(key=lambda x: x['last_scan_date'], reverse=True)
        elif sort_option == "Давно не скановані": sorted_kws.sort(key=lambda x: x['last_scan_date'], reverse=False)

        # Збираємо ID поточного списку для логіки "Select All"
        current_page_ids = [str(k['id']) for k in sorted_kws]

        # --- 3. STATE MANAGEMENT (CALLBACKS) ---
        # Ці функції запускаються ПЕРЕД рендерингом, коли користувач щось клікає

        def master_checkbox_change():
            """Коли клікають 'Всі': проставляємо це значення всім видимим елементам"""
            # Отримуємо новий стан чекбокса "Всі"
            new_state = st.session_state.select_all_master_key
            for kid in current_page_ids:
                st.session_state[f"chk_{kid}"] = new_state

        def child_checkbox_change():
            """Коли клікають окремий рядок: перевіряємо, чи треба зняти галочку 'Всі'"""
            # Якщо хоча б один з видимих елементів False -> Master має бути False
            all_selected = True
            for kid in current_page_ids:
                if not st.session_state.get(f"chk_{kid}", False):
                    all_selected = False
                    break
            st.session_state.select_all_master_key = all_selected

        # Ініціалізація стану для кожного рядка (якщо його ще немає)
        for kid in current_page_ids:
            key = f"chk_{kid}"
            if key not in st.session_state:
                st.session_state[key] = False

        # Ініціалізація майстер-ключа
        if "select_all_master_key" not in st.session_state:
            st.session_state.select_all_master_key = False

        # --- 4. ПАНЕЛЬ ДІЙ ---
        with st.container(border=True):
            c_check, c_models, c_btn = st.columns([0.5, 3, 1.5])
            
            with c_check:
                st.write("") 
                # MASTER CHECKBOX
                # Важливо: ми не передаємо value, бо key вже керує станом
                st.checkbox("Всі", key="select_all_master_key", on_change=master_checkbox_change)
            
            with c_models:
                all_models = list(MODEL_MAPPING.keys())
                # Використовуємо окремий ключ для моделей, щоб не конфліктував
                bulk_models = st.multiselect(
                    "ЛЛМ для запуску:", 
                    all_models, 
                    default=all_models, 
                    label_visibility="collapsed", 
                    key="bulk_models_selector_v6"
                )
            
            with c_btn:
                if st.button("🚀 Аналізувати обрані", use_container_width=True, type="primary"):
                    selected_texts = []
                    # Збираємо тільки ті, що True в session_state
                    for k in sorted_kws:
                        if st.session_state.get(f"chk_{k['id']}", False):
                            selected_texts.append(k['keyword_text'])
                    
                    if selected_texts:
                        try:
                            if 'n8n_trigger_analysis' in globals():
                                my_bar = st.progress(0, text="Ініціалізація...")
                                total = len(selected_texts)
                                for i, txt in enumerate(selected_texts):
                                    my_bar.progress((i / total), text=f"Відправка: {txt}...")
                                    n8n_trigger_analysis(proj_data["id"], [txt], proj_data.get("brand_name"), models=bulk_models)
                                    time.sleep(0.2)
                                my_bar.progress(1.0, text="Готово!")
                                st.success(f"Запущено {total} завдань.")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error("Функція запуску не знайдена.")
                        except Exception as e:
                            st.error(f"Помилка: {e}")
                    else:
                        st.warning("Оберіть хоча б один запит.")

        # --- 5. ТАБЛИЦЯ (RENDER LIST) ---
        h_chk, h_num, h_txt, h_cron, h_date, h_act = st.columns([0.4, 0.5, 3.2, 2, 1.2, 1.3])
        h_txt.markdown("**Запит**")
        h_cron.markdown("**Автозапуск**")
        h_date.markdown("**Останній аналіз**")
        h_act.markdown("**Видалити**")

        allow_cron_global = proj_data.get('allow_cron', False)

        for idx, k in enumerate(sorted_kws, start=1):
            k_id_str = str(k['id'])
            
            with st.container(border=True):
                c1, c2, c3, c4, c5, c6 = st.columns([0.4, 0.5, 3.2, 2, 1.2, 1.3])
                
                with c1:
                    st.write("") 
                    # ROW CHECKBOX
                    # ВАЖЛИВО: Ніякого `value=...`. Стан повністю керується через key в session_state.
                    # on_change викликає перевірку, чи треба зняти галочку "Всі"
                    st.checkbox("", key=f"chk_{k_id_str}", on_change=child_checkbox_change)
                
                with c2:
                    st.markdown(f"<div class='green-number'>{idx}</div>", unsafe_allow_html=True)
                
                with c3:
                    if st.button(k['keyword_text'], key=f"lnk_{k_id_str}", help="Деталі"):
                        st.session_state["focus_keyword_id"] = k["id"]
                        st.rerun()
                
                with c4:
                    # Логіка автозапуску (БД Toggle)
                    # Тут ми використовуємо suffix_val, щоб уникнути конфліктів ключів при оновленнях
                    cron_c1, cron_c2 = st.columns([0.8, 1.2])
                    is_auto_db = k.get('is_auto_scan', False)
                    
                    with cron_c1:
                        if allow_cron_global:
                            # Це toggle бази даних, він не залежить від чекбоксів вибору
                            toggle_key = f"auto_{k_id_str}_{suffix_val}"
                            new_auto = st.toggle("Авто", value=is_auto_db, key=toggle_key, label_visibility="collapsed")
                            if new_auto != is_auto_db:
                                update_kw_field(k['id'], "is_auto_scan", new_auto)
                        else:
                            st.toggle("Авто", value=False, key=f"auto_dis_{k_id_str}", disabled=True, label_visibility="collapsed")
                            st.caption("🔒")

                    with cron_c2:
                        if allow_cron_global and (is_auto_db or new_auto): # Показуємо, якщо увімкнено (навіть щойно)
                            current_freq = k.get('frequency', 'daily')
                            freq_options = ["daily", "weekly", "monthly"]
                            try: idx_f = freq_options.index(current_freq)
                            except: idx_f = 0
                            
                            freq_key = f"freq_{k_id_str}_{suffix_val}"
                            new_freq = st.selectbox("Freq", freq_options, index=idx_f, key=freq_key, label_visibility="collapsed")
                            if new_freq != current_freq:
                                update_kw_field(k['id'], "frequency", new_freq)

                with c5:
                    st.write("")
                    date_iso = k.get('last_scan_date')
                    formatted_date = format_kyiv_time(date_iso)
                    st.caption(f"{formatted_date}")

                with c6:
                    st.write("")
                    # Логіка видалення
                    del_confirm_key = f"del_confirm_{k_id_str}"
                    if del_confirm_key not in st.session_state: st.session_state[del_confirm_key] = False

                    if not st.session_state[del_confirm_key]:
                        if st.button("🗑️", key=f"pre_del_{k_id_str}"):
                            st.session_state[del_confirm_key] = True
                            st.rerun()
                    else:
                        dc1, dc2 = st.columns(2)
                        if dc1.button("✅", key=f"yes_del_{k_id_str}", type="primary"):
                            try:
                                supabase.table("scan_results").delete().eq("keyword_id", k["id"]).execute()
                                supabase.table("keywords").delete().eq("id", k["id"]).execute()
                                st.success("OK")
                                st.session_state[del_confirm_key] = False
                                time.sleep(0.5)
                                st.rerun()
                            except:
                                st.error("Error")
                        if dc2.button("❌", key=f"no_del_{k_id_str}"):
                            st.session_state[del_confirm_key] = False
                            st.rerun()

    # Запускаємо фрагмент
    render_live_dashboard(keywords, proj, update_suffix)

# =========================
# 9. SIDEBAR
# =========================

def show_sources_page():
    """
    Сторінка джерел.
    ВЕРСІЯ: FIXED ENUM & DESIGN UPDATE.
    1. Виправлено помилку 'invalid input value for enum'.
    2. Дизайн редагування змінено на стиль 'список карток' (як у запитах).
    3. Додано мапінг типів (Ukr -> Eng).
    """
    import pandas as pd
    import plotly.express as px
    import streamlit as st
    import time
    from urllib.parse import urlparse
    
    # Підключення
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

    # --- CSS для зелених номерів (дублюємо тут про всяк випадок) ---
    st.markdown("""
    <style>
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
    </style>
    """, unsafe_allow_html=True)

    st.title("🔗 Джерела")

    # --- MAPPING ТИПІВ (UI -> DB) ---
    TYPE_UI_TO_DB = {
        "Веб-сайт": "website",
        "Соціальні мережі": "social",
        "Стаття": "article",
        "Інше": "other"
    }
    # Зворотній мапінг (DB -> UI)
    TYPE_DB_TO_UI = {v: k for k, v in TYPE_UI_TO_DB.items()}

    # ==============================================================================
    # 1. ОТРИМАННЯ ДАНИХ (Скан результати)
    # ==============================================================================
    try:
        # Keywords
        kw_resp = supabase.table("keywords").select("id, keyword_text").eq("project_id", proj["id"]).execute()
        kw_map = {k['id']: k['keyword_text'] for k in kw_resp.data} if kw_resp.data else {}

        # Scan Results
        scan_resp = supabase.table("scan_results")\
            .select("id, provider, created_at, keyword_id")\
            .eq("project_id", proj["id"])\
            .execute()
        
        scan_meta = {} 
        scan_ids = []
        
        PROVIDER_MAP = {
            "perplexity": "Perplexity",
            "gpt-4o": "OpenAI GPT", "gpt-4": "OpenAI GPT",
            "gemini-1.5-pro": "Google Gemini", "gemini": "Google Gemini"
        }

        if scan_resp.data:
            for s in scan_resp.data:
                scan_ids.append(s['id'])
                raw_p = s.get('provider', '').lower()
                clean_p = "Інше"
                for k, v in PROVIDER_MAP.items():
                    if k in raw_p:
                        clean_p = v
                        break
                
                scan_meta[s['id']] = {
                    'provider': clean_p,
                    'date': s['created_at'],
                    'keyword_text': kw_map.get(s['keyword_id'], "Невідомий запит")
                }
        
        # Extracted Sources
        df_master = pd.DataFrame()
        if scan_ids:
            sources_resp = supabase.table("extracted_sources").select("*").in_("scan_result_id", scan_ids).execute()
            if sources_resp.data:
                df_master = pd.DataFrame(sources_resp.data)
                df_master['provider'] = df_master['scan_result_id'].map(lambda x: scan_meta.get(x, {}).get('provider', 'Інше'))
                df_master['keyword_text'] = df_master['scan_result_id'].map(lambda x: scan_meta.get(x, {}).get('keyword_text', ''))
                df_master['scan_date'] = df_master['scan_result_id'].map(lambda x: scan_meta.get(x, {}).get('date'))
                
                if 'domain' not in df_master.columns:
                    df_master['domain'] = df_master['url'].apply(lambda x: urlparse(x).netloc if x else "unknown")

    except Exception as e:
        st.error(f"Помилка завантаження даних: {e}")
        df_master = pd.DataFrame()

    # ==============================================================================
    # 2. WHITELIST LOGIC (ПРАВИЛЬНЕ ЧИТАННЯ)
    # ==============================================================================
    try:
        # 🔥 FIX: Читаємо з таблиці official_assets
        oa_resp = supabase.table("official_assets").select("domain_or_url, type").eq("project_id", proj["id"]).execute()
        raw_assets = oa_resp.data if oa_resp.data else []
    except Exception as e:
        raw_assets = []

    # Формуємо список для логіки (для підрахунку)
    assets_list_dicts = []
    for item in raw_assets:
        # Конвертуємо тип з БД в UI (english -> ukrainian)
        db_type = item.get("type", "website")
        ui_type = TYPE_DB_TO_UI.get(db_type, "Веб-сайт")
        
        assets_list_dicts.append({
            "Домен": item.get("domain_or_url", ""), 
            "Мітка": ui_type
        })
    
    OFFICIAL_DOMAINS = [d["Домен"].lower().strip() for d in assets_list_dicts if d["Домен"]]

    # Функція перевірки
    def check_is_official(url):
        if not url: return False
        u_str = str(url).lower()
        for od in OFFICIAL_DOMAINS:
            if od in u_str: return True
        return False

    if not df_master.empty:
        df_master['is_official_dynamic'] = df_master['url'].apply(check_is_official)

    # ==============================================================================
    # 3. ВКЛАДКИ
    # ==============================================================================
    tab1, tab2, tab3 = st.tabs(["📊 Офіційні ресурси бренду", "🌐 Ренкінг доменів", "🔗 Посилання"])

    # --- TAB 1: АНАЛІЗ ОХОПЛЕННЯ ---
    with tab1:
        st.markdown("#### 📊 Аналіз охоплення офіційних ресурсів")
        
        if not df_master.empty:
            total_rows = len(df_master)
            off_rows = df_master[df_master['is_official_dynamic'] == True]
            ext_rows = df_master[df_master['is_official_dynamic'] == False]
            
            def get_counts(df_sub):
                cnt = len(df_sub)
                if cnt == 0: return 0, 0, 0, 0
                p_c = len(df_sub[df_sub['provider'] == 'Perplexity'])
                g_c = len(df_sub[df_sub['provider'] == 'OpenAI GPT'])
                gem_c = len(df_sub[df_sub['provider'] == 'Google Gemini'])
                return cnt, p_c, g_c, gem_c

            tot_all, tot_p, tot_g, tot_gem = get_counts(df_master)
            off_all, off_p, off_g, off_gem = get_counts(off_rows)
            
            c_chart, c_stats = st.columns([2.5, 1.5], vertical_alignment="center")
            
            with c_chart:
                if total_rows > 0:
                    fig = px.pie(
                        names=["Офіційні", "Зовнішні"], 
                        values=[off_all, len(ext_rows)],
                        hole=0.55, 
                        color_discrete_sequence=["#00C896", "#E0E0E0"]
                    )
                    fig.update_layout(margin=dict(t=10, b=10, l=10, r=10), height=350, showlegend=True)
                    fig.update_traces(textposition='inside', textinfo='percent+label')
                    st.plotly_chart(fig, use_container_width=True, key="unique_chart_key_sources_1")
                else:
                    st.info("Немає даних.")

            with c_stats:
                st.markdown(f"""
                <div style="margin-bottom: 20px; padding:20px; border:1px solid #eee; border-radius:12px; background:white; box-shadow: 0 2px 5px rgba(0,0,0,0.05);">
                    <div style="color:#888; font-size:13px; font-weight:700; text-transform:uppercase; margin-bottom:5px;">Всього посилань</div>
                    <div style="font-size:32px; font-weight:800; color:#333; line-height:1;">{tot_all}</div>
                    <div style="margin-top:10px; font-size:12px; color:#555; display:flex; flex-direction:column; gap:3px;">
                        <div>🔹 Perplexity: <b>{tot_p}</b></div>
                        <div>🔸 OpenAI GPT: <b>{tot_g}</b></div>
                        <div>✨ Google Gemini: <b>{tot_gem}</b></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                st.markdown(f"""
                <div style="padding:20px; border:1px solid #00C896; border-radius:12px; background:#f0fdf9; box-shadow: 0 2px 5px rgba(0,200,150,0.1);">
                    <div style="color:#007a5c; font-size:13px; font-weight:700; text-transform:uppercase; margin-bottom:5px;">З них офіційні</div>
                    <div style="font-size:32px; font-weight:800; color:#00C896; line-height:1;">{off_all}</div>
                    <div style="margin-top:10px; font-size:12px; color:#005c45; display:flex; flex-direction:column; gap:3px;">
                        <div>🔹 Perplexity: <b>{off_p}</b></div>
                        <div>🔸 OpenAI GPT: <b>{off_g}</b></div>
                        <div>✨ Google Gemini: <b>{off_gem}</b></div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

        else:
            st.info("Дані сканування відсутні.")

        st.divider()

        # --- РЕДАКТОР WHITELIST (НОВИЙ ДИЗАЙН) ---
        st.subheader("⚙️ Керування списком (Whitelist)")
        
        if "edit_whitelist_mode" not in st.session_state:
            st.session_state["edit_whitelist_mode"] = False
        
        # Ініціалізація змінної для редагування
        if "temp_assets" not in st.session_state:
            st.session_state["temp_assets"] = []

        # --- ВІДОБРАЖЕННЯ ТАБЛИЦІ (View Mode) ---
        if not st.session_state["edit_whitelist_mode"]:
            # Готуємо DataFrame для перегляду
            if assets_list_dicts:
                df_assets = pd.DataFrame(assets_list_dicts)
            else:
                df_assets = pd.DataFrame(columns=["Домен", "Мітка"])

            # Рахуємо статистику
            if not df_master.empty:
                def get_stat_whitelist(dom):
                    matches = df_master[df_master['url'].astype(str).str.contains(dom.lower(), case=False, na=False)]
                    return len(matches)
                df_assets['Згадок'] = df_assets['Домен'].apply(get_stat_whitelist)
            else:
                df_assets['Згадок'] = 0

            st.dataframe(
                df_assets,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Домен": st.column_config.TextColumn("Домен / URL", width="medium"),
                    "Мітка": st.column_config.TextColumn("Тип ресурсу", width="small"),
                    "Згадок": st.column_config.NumberColumn("Знайдено разів", format="%d")
                }
            )
            
            if st.button("✏️ Редагувати список"):
                st.session_state["edit_whitelist_mode"] = True
                # Завантажуємо поточні дані в temp_assets для редагування
                st.session_state["temp_assets"] = assets_list_dicts.copy()
                st.rerun()
        
        # --- РЕЖИМ РЕДАГУВАННЯ (Custom Design) ---
        else:
            st.info("Додайте або видаліть домени. Натисніть 'Зберегти' для застосування змін.")
            
            # Якщо список пустий, додаємо один порожній рядок
            if not st.session_state["temp_assets"]:
                st.session_state["temp_assets"].append({"Домен": "", "Мітка": "Веб-сайт"})

            # Відображаємо список карток
            for i, asset in enumerate(st.session_state["temp_assets"]):
                with st.container(border=True):
                    c_num, c_dom, c_type, c_del = st.columns([0.5, 5, 3, 1])
                    
                    with c_num:
                        st.markdown(f"<div class='green-number'>{i+1}</div>", unsafe_allow_html=True)
                    
                    with c_dom:
                        new_domain = st.text_input(
                            "Домен", 
                            value=asset["Домен"], 
                            key=f"asset_dom_{i}", 
                            label_visibility="collapsed",
                            placeholder="example.com"
                        )
                        st.session_state["temp_assets"][i]["Домен"] = new_domain
                    
                    with c_type:
                        new_type = st.selectbox(
                            "Тип", 
                            options=list(TYPE_UI_TO_DB.keys()), 
                            index=list(TYPE_UI_TO_DB.keys()).index(asset["Мітка"]) if asset["Мітка"] in TYPE_UI_TO_DB else 0,
                            key=f"asset_type_{i}", 
                            label_visibility="collapsed"
                        )
                        st.session_state["temp_assets"][i]["Мітка"] = new_type

                    with c_del:
                        if st.button("🗑️", key=f"del_asset_{i}"):
                            st.session_state["temp_assets"].pop(i)
                            st.rerun()

            # Кнопка додавання
            if st.button("➕ Додати джерело"):
                st.session_state["temp_assets"].append({"Домен": "", "Мітка": "Веб-сайт"})
                st.rerun()

            st.divider()

            # Кнопки дії
            c1, c2 = st.columns([1, 4])
            with c1:
                if st.button("💾 Зберегти", type="primary"):
                    try:
                        # 1. Видаляємо старі записи
                        supabase.table("official_assets").delete().eq("project_id", proj["id"]).execute()
                        
                        # 2. Формуємо нові дані (конвертуємо UI -> DB)
                        insert_data = []
                        for item in st.session_state["temp_assets"]:
                            d_val = str(item["Домен"]).strip()
                            if d_val:
                                # Конвертація "Веб-сайт" -> "website"
                                db_type_val = TYPE_UI_TO_DB.get(item["Мітка"], "website")
                                
                                insert_data.append({
                                    "project_id": proj["id"],
                                    "domain_or_url": d_val,
                                    "type": db_type_val
                                })
                        
                        # 3. Вставляємо
                        if insert_data:
                            supabase.table("official_assets").insert(insert_data).execute()
                            
                        st.success("Список оновлено!")
                        st.session_state["edit_whitelist_mode"] = False
                        time.sleep(1)
                        st.rerun()
                    except Exception as e:
                        st.error(f"Помилка збереження: {e}")
            with c2:
                if st.button("❌ Скасувати"):
                    st.session_state["edit_whitelist_mode"] = False
                    st.rerun()

    # --- TAB 2: РЕНКІНГ ---
    with tab2:
        st.markdown("#### 🏆 Ренкінг доменів")
        if not df_master.empty:
            all_kws = sorted(df_master['keyword_text'].unique())
            sel_kws_rank = st.multiselect("🔍 Фільтр по запитах:", all_kws, key="rank_kw_filter")
            
            df_rank_view = df_master.copy()
            if sel_kws_rank:
                df_rank_view = df_rank_view[df_rank_view['keyword_text'].isin(sel_kws_rank)]
            
            if not df_rank_view.empty:
                pivot_df = df_rank_view.pivot_table(
                    index='domain', columns='provider', values='mention_count', aggfunc='sum', fill_value=0
                ).reset_index()
                
                pivot_df['Всього'] = pivot_df.sum(axis=1, numeric_only=True)
                for col in ["Perplexity", "OpenAI GPT", "Google Gemini"]:
                    if col not in pivot_df.columns: pivot_df[col] = 0
                
                def get_meta(dom):
                    is_off = "Зовнішній"
                    for od in OFFICIAL_DOMAINS:
                        if od in dom.lower():
                            is_off = "Офіційний"
                            break
                    dates = df_rank_view[df_rank_view['domain'] == dom]['scan_date']
                    first = dates.min() if not dates.empty else None
                    first_str = pd.to_datetime(first).strftime("%Y-%m-%d") if first else "-"
                    return is_off, first_str

                pivot_df[['Тип', 'Вперше знайдено']] = pivot_df['domain'].apply(lambda x: pd.Series(get_meta(x)))
                pivot_df = pivot_df.sort_values("Всього", ascending=False).reset_index(drop=True)
                
                cols_order = ["domain", "Тип", "Всього", "Perplexity", "OpenAI GPT", "Google Gemini", "Вперше знайдено"]
                final_cols = [c for c in cols_order if c in pivot_df.columns]
                
                st.dataframe(
                    pivot_df[final_cols],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "domain": "Домен",
                        "Всього": st.column_config.NumberColumn(format="%d"),
                        "Perplexity": st.column_config.NumberColumn(format="%d"),
                        "OpenAI GPT": st.column_config.NumberColumn(format="%d"),
                        "Google Gemini": st.column_config.NumberColumn(format="%d"),
                    }
                )
            else:
                st.warning("Даних немає.")
        else:
            st.info("Дані відсутні.")

    # --- TAB 3: ПОСИЛАННЯ ---
    with tab3:
        st.markdown("#### 🔗 Детальний список посилань")
        if not df_master.empty:
            c_f1, c_f2 = st.columns([1, 1])
            with c_f1: sel_kws_links = st.multiselect("🔍 Фільтр по запитах:", all_kws, key="links_kw_filter")
            with c_f2: search_url = st.text_input("🔎 Пошук URL:", key="links_search")
            
            c_f3, c_f4 = st.columns(2)
            with c_f3: type_filter = st.selectbox("Тип ресурсу:", ["Всі", "Офіційні", "Зовнішні"], key="links_type_filter")
            
            df_links_view = df_master.copy()
            if sel_kws_links: df_links_view = df_links_view[df_links_view['keyword_text'].isin(sel_kws_links)]
            if search_url: df_links_view = df_links_view[df_links_view['url'].astype(str).str.contains(search_url, case=False)]
            if type_filter == "Офіційні": df_links_view = df_links_view[df_links_view['is_official_dynamic'] == True]
            elif type_filter == "Зовнішні": df_links_view = df_links_view[df_links_view['is_official_dynamic'] == False]

            if not df_links_view.empty:
                pivot_links = df_links_view.pivot_table(
                    index=['url', 'domain', 'is_official_dynamic'],
                    columns='provider', values='mention_count', aggfunc='sum', fill_value=0
                ).reset_index()
                
                pivot_links['Всього'] = pivot_links.sum(axis=1, numeric_only=True)
                for col in ["Perplexity", "OpenAI GPT", "Google Gemini"]:
                    if col not in pivot_links.columns: pivot_links[col] = 0
                
                pivot_links['Тип'] = pivot_links['is_official_dynamic'].apply(lambda x: "Офіційні" if x else "Зовнішні")
                pivot_links = pivot_links.sort_values("Всього", ascending=False).reset_index(drop=True)
                
                cols_order = ["url", "domain", "Тип", "Всього", "Perplexity", "OpenAI GPT", "Google Gemini"]
                final_cols = [c for c in cols_order if c in pivot_links.columns]
                
                st.dataframe(
                    pivot_links[final_cols],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "url": st.column_config.LinkColumn("Посилання", width="large"),
                        "Всього": st.column_config.NumberColumn(format="%d"),
                        "Perplexity": st.column_config.NumberColumn(format="%d"),
                        "OpenAI GPT": st.column_config.NumberColumn(format="%d"),
                        "Google Gemini": st.column_config.NumberColumn(format="%d"),
                    }
                )
            else:
                st.warning("Нічого не знайдено.")
        else:
            st.info("Дані відсутні.")


def show_history_page():
    """
    Сторінка історії сканувань.
    ВЕРСІЯ: PROFILES MAPPING.
    1. Бере user_email з scan_results.
    2. Шукає власника в таблиці 'profiles'.
    3. Формує ПІБ (first_name + last_name).
    """
    import pandas as pd
    import streamlit as st
    from datetime import datetime, timedelta
    import pytz
    import math

    # Налаштування часового поясу
    KYIV_TZ = pytz.timezone('Europe/Kiev')

    # Функція для скидання сторінки
    def reset_page():
        st.session_state.history_page_number = 1

    if 'history_page_number' not in st.session_state:
        st.session_state.history_page_number = 1

    # --- 1. ПІДКЛЮЧЕННЯ ---
    if 'supabase' in st.session_state:
        supabase = st.session_state['supabase']
    elif 'supabase' in globals():
        supabase = globals()['supabase']
    else:
        st.error("🚨 Помилка: Змінна 'supabase' не знайдена.")
        return

    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку оберіть проект.")
        return

    st.title("📜 Історія сканувань")

    # --- 2. ОТРИМАННЯ ДАНИХ ---
    with st.spinner("Завантаження історії..."):
        try:
            # 1. Keywords
            kw_resp = supabase.table("keywords").select("id, keyword_text").eq("project_id", proj["id"]).execute()
            kw_map = {k['id']: k['keyword_text'] for k in kw_resp.data} if kw_resp.data else {}

            # 2. Scans (Беремо user_email)
            scans_resp = supabase.table("scan_results")\
                .select("id, created_at, provider, keyword_id, user_email")\
                .eq("project_id", proj["id"])\
                .order("created_at", desc=True)\
                .limit(1000)\
                .execute()
            
            scans_data = scans_resp.data if scans_resp.data else []
            
            if not scans_data:
                st.info("Історія сканувань порожня.")
                return

            scan_ids = [s['id'] for s in scans_data]

            # 🔥 3. ОТРИМАННЯ ПІБ З ТАБЛИЦІ PROFILES
            unique_emails = list(set([s['user_email'] for s in scans_data if s.get('user_email')]))
            email_to_name_map = {}

            if unique_emails:
                try:
                    # ⚠️ Змінено таблицю на 'profiles'
                    p_resp = supabase.table("profiles")\
                        .select("email, first_name, last_name")\
                        .in_("email", unique_emails)\
                        .execute()
                    
                    if p_resp.data:
                        for p in p_resp.data:
                            f_n = p.get('first_name', '') or ''
                            l_n = p.get('last_name', '') or ''
                            full_n = f"{f_n} {l_n}".strip()
                            
                            # Якщо ім'я знайдене, записуємо його в мапу
                            if full_n and p.get('email'):
                                email_to_name_map[p['email']] = full_n
                except Exception:
                    # Якщо таблиці profiles немає або помилка доступу
                    pass

            # 4. Mentions
            m_resp = supabase.table("brand_mentions")\
                .select("scan_result_id, is_my_brand, mention_count")\
                .in_("scan_result_id", scan_ids)\
                .execute()
            mentions_df = pd.DataFrame(m_resp.data) if m_resp.data else pd.DataFrame()

            # 5. Sources
            s_resp = supabase.table("extracted_sources")\
                .select("scan_result_id, is_official")\
                .in_("scan_result_id", scan_ids)\
                .execute()
            sources_df = pd.DataFrame(s_resp.data) if s_resp.data else pd.DataFrame()

        except Exception as e:
            if "column scan_results.user_email does not exist" in str(e):
                st.error("⚠️ Відсутня колонка `user_email` у таблиці scan_results.")
            else:
                st.error(f"Помилка завантаження даних: {e}")
            return

    # --- 3. ОБРОБКА ДАНИХ ---
    df_scans = pd.DataFrame(scans_data)

    # 🔥 ЛОГІКА ІНІЦІАТОРА
    def resolve_initiator(email_val):
        # 1. Якщо емейл пустий -> Авто
        if pd.isna(email_val) or str(email_val).strip() == "" or str(email_val).lower() == "none":
            return "🤖 Автосканування"
        
        # 2. Якщо ми знайшли ім'я у profiles -> Виводимо ПІБ
        if email_val in email_to_name_map:
            return f"👤 {email_to_name_map[email_val]}"
        
        # 3. Якщо імені не знайшли (профіль не заповнений) -> Виводимо Email
        return f"👤 {email_val}"
    
    # Застосовуємо, якщо колонка є
    if 'user_email' in df_scans.columns:
        df_scans['initiator'] = df_scans['user_email'].apply(resolve_initiator)
    else:
        df_scans['initiator'] = "🤖 Автосканування"

    # Провайдери
    PROVIDER_MAP = {"gpt-4o": "OpenAI", "gpt-4-turbo": "OpenAI", "gemini-1.5-pro": "Gemini", "perplexity": "Perplexity"}
    df_scans['provider'] = df_scans['provider'].replace(PROVIDER_MAP)
    
    # Ключові слова
    df_scans['keyword'] = df_scans['keyword_id'].map(kw_map).fillna("Видалений запит")
    
    # Timezone Fix
    df_scans['created_at_dt'] = pd.to_datetime(df_scans['created_at']).dt.tz_convert(KYIV_TZ)
    
    # Merge (Безпечне злиття)
    if not mentions_df.empty:
        brands_count = mentions_df.groupby('scan_result_id').size().reset_index(name='total_brands')
        my_mentions = mentions_df[mentions_df['is_my_brand'] == True].groupby('scan_result_id')['mention_count'].sum().reset_index(name='my_mentions_count')
        
        df_scans = df_scans.merge(brands_count, left_on='id', right_on='scan_result_id', how='left')
        if 'scan_result_id' in df_scans.columns: df_scans = df_scans.drop(columns=['scan_result_id'])
        
        df_scans = df_scans.merge(my_mentions, left_on='id', right_on='scan_result_id', how='left')
        if 'scan_result_id' in df_scans.columns: df_scans = df_scans.drop(columns=['scan_result_id'])
    else:
        df_scans['total_brands'] = 0
        df_scans['my_mentions_count'] = 0

    if not sources_df.empty:
        links_count = sources_df.groupby('scan_result_id').size().reset_index(name='total_links')
        off_count = sources_df[sources_df['is_official'] == True].groupby('scan_result_id').size().reset_index(name='official_links')
        
        df_scans = df_scans.merge(links_count, left_on='id', right_on='scan_result_id', how='left')
        if 'scan_result_id' in df_scans.columns: df_scans = df_scans.drop(columns=['scan_result_id'])
        
        df_scans = df_scans.merge(off_count, left_on='id', right_on='scan_result_id', how='left')
        if 'scan_result_id' in df_scans.columns: df_scans = df_scans.drop(columns=['scan_result_id'])
    else:
        df_scans['total_links'] = 0
        df_scans['official_links'] = 0

    df_scans = df_scans.fillna(0)

    # --- 4. ФІЛЬТРИ ---
    st.markdown("### 🔍 Фільтрація")
    
    now_kyiv = datetime.now(KYIV_TZ).date()
    
    if not df_scans.empty:
        min_date_avail = df_scans['created_at_dt'].min().date()
        max_date_avail = max(df_scans['created_at_dt'].max().date(), now_kyiv) + timedelta(days=1)
    else:
        min_date_avail = now_kyiv
        max_date_avail = now_kyiv + timedelta(days=1)

    c1, c2, c3, c4 = st.columns([1, 1.2, 1, 0.8])
    
    with c1:
        all_providers = df_scans['provider'].unique().tolist()
        sel_providers = st.multiselect("Модель", all_providers, default=all_providers, on_change=reset_page)
    
    with c2:
        default_start = now_kyiv - timedelta(days=30)
        sel_dates = st.date_input(
            "Період",
            value=(default_start, now_kyiv),
            min_value=min_date_avail - timedelta(days=365),
            max_value=max_date_avail
        )
        
    with c3:
        sort_opts = ["Найновіші", "Найстаріші", "Більше згадок", "Офіц. джерела"]
        sel_sort = st.selectbox("Сортування", sort_opts, on_change=reset_page)

    with c4:
        rows_per_page = st.selectbox("Рядків на стор.", [10, 20, 50, 100, 200], index=0, on_change=reset_page)

    # --- ЗАСТОСУВАННЯ ФІЛЬТРІВ ---
    mask = df_scans['provider'].isin(sel_providers)
    
    if isinstance(sel_dates, tuple):
        if len(sel_dates) == 2:
            start_d, end_d = sel_dates
            mask &= (df_scans['created_at_dt'].dt.date >= start_d)
            mask &= (df_scans['created_at_dt'].dt.date <= end_d)
        elif len(sel_dates) == 1:
            mask &= (df_scans['created_at_dt'].dt.date == sel_dates[0])
        
    df_filtered = df_scans[mask].copy()

    # Сортування
    if sel_sort == "Найновіші": df_filtered = df_filtered.sort_values('created_at_dt', ascending=False)
    elif sel_sort == "Найстаріші": df_filtered = df_filtered.sort_values('created_at_dt', ascending=True)
    elif sel_sort == "Більше згадок": df_filtered = df_filtered.sort_values('my_mentions_count', ascending=False)
    elif sel_sort == "Офіц. джерела": df_filtered = df_filtered.sort_values('official_links', ascending=False)

    # --- 5. ПАГІНАЦІЯ ---
    total_rows = len(df_filtered)
    total_pages = math.ceil(total_rows / rows_per_page)
    
    if st.session_state.history_page_number > total_pages:
        st.session_state.history_page_number = max(1, total_pages)
    
    current_page = st.session_state.history_page_number
    start_idx = (current_page - 1) * rows_per_page
    end_idx = start_idx + rows_per_page
    
    df_display_page = df_filtered.iloc[start_idx:end_idx].copy()

    # --- 6. ВІДОБРАЖЕННЯ (AUTO HEIGHT) ---
    st.divider()
    
    p_col1, p_col2, p_col3 = st.columns([1, 2, 1])
    with p_col1:
        if current_page > 1:
            if st.button("⬅️ Попередня", key="hist_prev_top"):
                st.session_state.history_page_number -= 1
                st.rerun()
    with p_col2:
        st.markdown(f"<div style='text-align: center; padding-top: 5px;'>Сторінка <b>{current_page}</b> з <b>{total_pages}</b> (Всього: {total_rows})</div>", unsafe_allow_html=True)
    with p_col3:
        if current_page < total_pages:
            if st.button("Наступна ➡️", key="hist_next_top"):
                st.session_state.history_page_number += 1
                st.rerun()

    if 'created_at_dt' in df_display_page.columns:
        df_display_page['created_at_dt'] = df_display_page['created_at_dt'].dt.strftime('%d.%m.%Y %H:%M')

    cols_to_show = ['created_at_dt', 'keyword', 'provider', 'total_brands', 'total_links', 'my_mentions_count', 'official_links', 'initiator']
    df_show = df_display_page[[c for c in cols_to_show if c in df_display_page.columns]]

    # Авто-висота
    dynamic_height = (len(df_show) * 35) + 38

    st.dataframe(
        df_show,
        use_container_width=True,
        hide_index=True,
        height=dynamic_height,
        column_config={
            "created_at_dt": "Дата (Kyiv)",
            "keyword": st.column_config.TextColumn("Запит", width="medium"),
            "provider": "LLM",
            "total_brands": st.column_config.NumberColumn("Бренди", help="Кількість знайдених конкурентів"),
            "total_links": st.column_config.NumberColumn("Посил.", help="Всього джерел"),
            "my_mentions_count": st.column_config.NumberColumn("Згадки", help="Згадки нашого бренду"),
            "official_links": st.column_config.NumberColumn("Офіц.", help="Офіційні джерела"),
            "initiator": st.column_config.TextColumn("Ініціатор", help="Хто запустив", width="medium")
        }
    )

    if total_rows > 10:
        st.write("")
        b_col1, b_col2, b_col3 = st.columns([1, 2, 1])
        with b_col1:
            if current_page > 1:
                if st.button("⬅️ Попередня", key="hist_prev_btm"):
                    st.session_state.history_page_number -= 1
                    st.rerun()
        with b_col3:
            if current_page < total_pages:
                if st.button("Наступна ➡️", key="hist_next_btm"):
                    st.session_state.history_page_number += 1
                    st.rerun()


def sidebar_menu():
    """
    Бокове меню навігації.
    ВЕРСІЯ: LOGO RESTORED.
    1. Логотип: Повернуто на місце (прибрано margin-top: -80px).
    2. Кнопка згортання: Активна, позиція top: 120px.
    3. Проект: Великий шрифт (20px), без лейбла.
    """
    from streamlit_option_menu import option_menu
    import streamlit as st
    
    # Отримуємо дані з сесії
    proj = st.session_state.get("current_project")
    user = st.session_state.get("user")
    user_details = st.session_state.get("user_details", {}) 
    
    user_email = user.email if user else "guest@virshi.ai"
    
    first_name = user_details.get("first_name", "")
    last_name = user_details.get("last_name", "")
    full_name = f"{first_name} {last_name}".strip()
    if not full_name: full_name = "Користувач"

    proj_name = proj.get("brand_name", "No Project") if proj else "Оберіть проект"
    proj_id = proj.get("id", "") if proj else ""
    proj_domain = proj.get("domain", "") if proj else ""

    with st.sidebar:
        # 🔥 CSS FIX
        st.markdown("""
            <style>
                /* Обнуляємо відступи контейнера, щоб контент був зверху */
                [data-testid="stSidebarBody"] {
                    padding-top: 0rem !important;
                }
                section[data-testid="stSidebar"] .block-container {
                    padding-top: 1rem !important;
                    margin-top: 0rem !important;
                }
                
                /* 🔥 КНОПКА ЗГОРТАННЯ */
                [data-testid="stSidebarHeader"] {
                    padding-top: 0rem !important;
                    height: 0rem !important;
                    
                    /* Фіксуємо позицію навпроти профілю */
                    position: absolute;
                    top: 135px !important; 
                    right: 10px !important;
                    
                    /* Робимо видимою та клікабельною */
                    z-index: 999999 !important;
                    pointer-events: auto !important;
                    background-color: transparent;
                    width: auto !important;
                }
                /* Колір іконки стрілочки */
                [data-testid="stSidebarHeader"] button {
                    color: #666 !important;
                }
                [data-testid="stSidebarHeader"] button:hover {
                    color: #00C896 !important;
                }
            </style>
        """, unsafe_allow_html=True)

        # 1. ЛОГОТИП + AI VISIBILITY (Нормальне позиціонування)
        st.markdown(f"""
            <div style="text-align: center; margin-bottom: 5px;">
                <img src="https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png" width="160" style="display: inline-block;">
                <div style="margin-top: 5px; font-size: 18px; font-weight: bold; color: #333; letter-spacing: 0.5px;">AI Visibility</div>
            </div>
            
            <div style="margin-top: 20px; border-top: 1px solid #E0E0E0;"></div>
            <div style="margin-top: 15px;"></div>
        """, unsafe_allow_html=True)

        # 2. ПРОФІЛЬ (Текст)
        st.markdown(f"""
        <div style='line-height: 1.2; margin-bottom: 10px; padding-right: 40px;'>
            <div style='font-size: 12px; color: rgba(49, 51, 63, 0.6); margin-bottom: 2px;'>Ви авторизовані як:</div>
            <div style='font-weight: 600; font-size: 16px; color: #31333F;'>{full_name}</div>
            <div style='font-size: 12px; color: rgba(49, 51, 63, 0.6);'>{user_email}</div>
        </div>
        """, unsafe_allow_html=True)
        
        st.write("") 

        # --- БЛОК ПРОЕКТУ ---
        logo_url = None
        backup_logo_url = None
        clean_d = None

        if proj and proj_domain:
            clean_d = proj_domain.lower().replace('https://', '').replace('http://', '').replace('www.', '')
            if '/' in clean_d: clean_d = clean_d.split('/')[0]
            
            logo_url = f"https://cdn.brandfetch.io/{clean_d}"
            backup_logo_url = f"https://www.google.com/s2/favicons?domain={clean_d}&sz=128"

        if proj and proj_name != "Оберіть проект":
            if logo_url:
                col_brand_img, col_brand_txt = st.columns([0.25, 0.75])
                with col_brand_img:
                    img_html = f'<img src="{logo_url}" style="width: 45px; height: 45px; object-fit: contain; border-radius: 5px; pointer-events: none;" onerror="this.onerror=null; this.src=\'{backup_logo_url}\';">'
                    st.markdown(img_html, unsafe_allow_html=True)
                
                with col_brand_txt:
                    # 🔥 ОНОВЛЕНО: Великий шрифт назви проекту
                    html_content = f"""
                    <div style='line-height: 1.1; display: flex; flex-direction: column; justify-content: center; height: 48px;'>
                        <div style='font-weight: bold; font-size: 20px; color: #31333F; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;'>{proj_name}</div>
                        <div style='font-size: 12px; color: #888;'>{clean_d if clean_d else ''}</div>
                    </div>
                    """
                    st.markdown(html_content, unsafe_allow_html=True)
            else:
                st.markdown(f"### 📁 {proj_name}")
                if clean_d: st.caption(clean_d)

        # Відступ перед меню
        st.markdown("<div style='height: 20px;'></div>", unsafe_allow_html=True)

        # 4. Меню
        options = [
            "Дашборд", 
            "Перелік запитів", 
            "Джерела", 
            "Конкуренти", 
            "Рекомендації", 
            "Історія сканувань", 
            "Звіти",              
            "FAQ",                
            "GPT-Visibility"
        ]
        
        icons = [
            "speedometer2", 
            "list-task", 
            "router", 
            "people", 
            "lightbulb", 
            "clock-history", 
            "file-earmark-text", 
            "question-circle",    
            "robot"
        ]

        if st.session_state.get("role") in ["admin", "super_admin"]:
            options.append("Адмін")
            icons.append("shield-lock")

        default_idx = 0
        redirect_target = st.session_state.get("force_redirect_to")
        
        if redirect_target and redirect_target in options:
            default_idx = options.index(redirect_target)
            del st.session_state["force_redirect_to"]
        
        menu_refresh_id = st.session_state.get("menu_id_counter", 0)

        selected = option_menu(
            "Меню",
            options,
            icons=icons,
            menu_icon="cast",
            default_index=default_idx,
            key=f"main_menu_nav_{menu_refresh_id}", 
            styles={
                "container": {"padding": "0!important", "background-color": "transparent"},
                "icon": {"color": "grey", "font-size": "16px"}, 
                "nav-link": {"font-size": "14px", "text-align": "left", "margin":"0px", "--hover-color": "#eee"},
                "nav-link-selected": {"background-color": "#00C896"},
            }
        )
        
        st.divider()

        # 5. Сапорт
        st.caption("Потрібна допомога?")
        st.markdown("📧 **hi@virshi.ai**")

        # 6. Статус
        if proj:
            st.write("")
            status = proj.get("status", "trial").upper()
            color = "orange" if status == "TRIAL" else "green" if status == "ACTIVE" else "red"
            
            st.markdown(f"Статус: **:{color}[{status}]**")
            st.caption(f"ID: `{proj_id}`")
            
            if st.session_state.get("is_impersonating"):
                st.info("🕵️ Admin Mode")

        st.write("")
        if st.button("🚪 Вийти з акаунту", use_container_width=True):
            if 'logout' in globals():
                logout()
            else:
                st.session_state.clear()
                st.rerun()

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
    ВЕРСІЯ: FINAL FIXES (RESET FIELDS, IMPORT URL, STATUS ERROR HANDLING).
    1. Tab 2: Виправлено очищення полів через динамічні ключі (fix 'cannot be modified').
    2. Tab 2: Додано імпорт запитів через URL.
    3. Tab 1: Обробка помилки ENUM для статусу 'blocked'.
    4. Tab 3: Проекти з нового рядка.
    """
    import pandas as pd
    import streamlit as st
    import numpy as np
    import requests
    import json
    import time
    import plotly.express as px
    import io
    import re

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

    # --- CSS ---
    st.markdown("""
    <style>
        .green-number { 
            background-color: #00C896; 
            color: white; 
            width: 24px; 
            height: 24px; 
            border-radius: 50%; 
            display: flex; 
            align-items: center; 
            justify-content: center; 
            font-weight: bold; 
            font-size: 12px; 
        }
        .del-kw-btn { color: #FF4B4B; cursor: pointer; font-weight: bold; }
    </style>
    """, unsafe_allow_html=True)

    # --- STATE ДЛЯ ОЧИЩЕННЯ ФОРМИ ---
    if "admin_reset_id" not in st.session_state:
        st.session_state["admin_reset_id"] = 0

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
            
            # Очистка кешу
            if "my_projects" in st.session_state: del st.session_state["my_projects"]
            if "all_projects_admin" in st.session_state: del st.session_state["all_projects_admin"]
            
            # Оновлення поточного
            if "current_project" in st.session_state and st.session_state["current_project"]:
                if st.session_state["current_project"]["id"] == proj_id:
                    st.session_state["current_project"][field] = val
                
            st.toast(f"✅ Оновлено: {field} -> {value}")
            time.sleep(0.5)
        except Exception as e:
            err_msg = str(e)
            if "invalid input value for enum" in err_msg:
                st.error(f"⚠️ Помилка БД: Статус '{value}' не додано в ENUM (тип даних) у Supabase. Зверніться до розробника БД.")
            else:
                st.error(f"Помилка оновлення: {err_msg}")

    # --- ВЕБХУК ---
    def trigger_keyword_generation(brand, domain, industry, products):
        payload = { "brand": brand, "domain": domain, "industry": industry, "products": products }
        headers = {"virshi-auth": "hi@virshi.ai2025"}
        try:
            response = requests.post(N8N_GEN_URL, json=payload, headers=headers, timeout=60)
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, dict):
                        if "prompts" in data: return data["prompts"]
                        if "keywords" in data: return data["keywords"]
                        return list(data.values()) if data else []
                    elif isinstance(data, list):
                        return data
                    return []
                except ValueError:
                    return []
            else:
                st.error(f"Error: {response.status_code}")
                return []
        except Exception as e:
            st.error(f"Connection error: {e}")
            return []

    # Ініціалізація списку
    if "new_proj_keywords" not in st.session_state:
        st.session_state["new_proj_keywords"] = [] 

    st.title("🛡️ Admin Panel (CRM)")

    # --- 1. ОТРИМАННЯ ДАНИХ ---
    try:
        projects_resp = supabase.table("projects").select("*").execute()
        projects_data = projects_resp.data if projects_resp.data else []

        kws_resp = supabase.table("keywords").select("project_id").execute()
        kws_df = pd.DataFrame(kws_resp.data) if kws_resp.data else pd.DataFrame()
        kw_counts = kws_df['project_id'].value_counts().to_dict() if not kws_df.empty else {}

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
                "email": u.get('email', '-'),
                "created_at": u.get('created_at', '')
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
        k1.metric("Всього проектів", total)
        k2.metric("Active", active)
        k3.metric("Trial", trial)
        k4.metric("Blocked", blocked)

    st.write("")

    # --- 3. ВКЛАДКИ ---
    tab_list, tab_create, tab_users = st.tabs(["📂 Список проектів", "➕ Створити проект", "👥 Користувачі & Права"])

    # ========================================================
    # TAB 1: СПИСОК ПРОЕКТІВ
    # ========================================================
    with tab_list:
        st.markdown("##### 🔍 Фільтрація та Пошук")
        
        fc1, fc2, fc3 = st.columns([2, 1.5, 1])
        with fc1:
            search_query = st.text_input("Пошук", placeholder="Назва, ID, домен, email власника", key="adm_search")
        with fc2:
            status_filter = st.multiselect("Статус", ["active", "trial", "blocked"], default=[], key="adm_status_filter", placeholder="Всі статуси")
        with fc3:
            sort_order = st.selectbox("Сортування", ["Найновіші", "Найстаріші"], key="adm_sort")

        st.divider()
        
        filtered_projects = []
        if projects_data:
            for p in projects_data:
                u_id = p.get('user_id')
                owner = user_map.get(u_id, {"full_name": "", "email": ""})
                
                p_name = p.get('brand_name') or p.get('project_name') or ""
                p_domain = p.get('domain') or ""
                p_id_str = str(p.get('id', ''))
                
                search_text = f"{p_name} {p_domain} {p_id_str} {owner['full_name']} {owner['email']}".lower()
                
                if search_query and search_query.lower() not in search_text: continue
                if status_filter and p.get('status', 'trial') not in status_filter: continue
                
                filtered_projects.append(p)

            reverse_sort = True if sort_order == "Найновіші" else False
            filtered_projects.sort(key=lambda x: x.get('created_at', ''), reverse=reverse_sort)

        # Header (Трохи змінили пропорції колонок, щоб вмістити лого)
        h0, h1, h_dash, h2, h3, h_cnt, h4, h5 = st.columns([0.3, 2.5, 0.4, 1.3, 1.2, 0.7, 0.9, 0.5])
        h0.markdown("**#**")
        h1.markdown("**Проект / Користувач**")
        h_dash.markdown("") 
        h2.markdown("**Статус**")
        h3.markdown("**Автосканування**")
        h_cnt.markdown("**Запитів**")
        h4.markdown("**Дата**")
        h5.markdown("**Дії**")
        st.markdown("<hr style='margin: 5px 0'>", unsafe_allow_html=True)

        if not filtered_projects: st.info("Проектів не знайдено.")

        for idx, p in enumerate(filtered_projects, 1):
            p_id = p['id']
            u_id = p.get('user_id')
            owner_info = user_map.get(u_id, {"full_name": "Невідомий", "role": "user", "email": "-"})
            
            raw_name = p.get('brand_name') or p.get('project_name')
            domain = p.get('domain', '')
            
            # Логіка очистки імені та домену
            if raw_name:
                clean_name = str(raw_name).replace('*', '').strip()
            else:
                clean_name = domain.replace('https://', '').replace('www.', '').split('/')[0] if domain else "Без назви"
# ---------------------------------------------------------
            # ЛОГІКА ЛОГОТИПУ (Brandfetch CDN + Google Fallback)
            # ---------------------------------------------------------
            logo_url = None
            backup_logo_url = None

            if domain:
                # Очистка домену від сміття
                clean_d = domain.lower().replace('https://', '').replace('http://', '').replace('www.', '')
                if '/' in clean_d: clean_d = clean_d.split('/')[0]
                
                # 1. Основне посилання (Brandfetch CDN)
                logo_url = f"https://cdn.brandfetch.io/{clean_d}"
                # 2. Резервне посилання (Google Favicons)
                backup_logo_url = f"https://www.google.com/s2/favicons?domain={clean_d}&sz=64"

            # Отримуємо кількість запитів
            k_count = kw_counts.get(p_id, 0)

            # ---------------------------------------------------------
            # ВІДОБРАЖЕННЯ В ТАБЛИЦІ
            # ---------------------------------------------------------
            with st.container():
                # Пропорції колонок
                c0, c1, c_dash, c2, c3, c_cnt, c4, c5 = st.columns([0.3, 2.5, 0.4, 1.3, 1.2, 0.7, 0.9, 0.5])

                with c0: st.caption(f"{idx}")

                with c1:
                    # Якщо є домен -> показуємо Лого + Назву
                    if logo_url:
                        sub_c1, sub_c2 = st.columns([0.15, 0.85])
                        
                        with sub_c1:
                            # 🔥 ФІКС "СИНЬОГО КОДУ": 
                            # Ми формуємо HTML в окремій змінній з одинарними лапками всередині.
                            # Це гарантує, що Python не заплутається в лапках.
                            img_html = f'<img src="{logo_url}" style="width: 30px; border-radius: 4px; pointer-events: none;" onerror="this.onerror=null; this.src=\'{backup_logo_url}\';">'
                            
                            st.markdown(img_html, unsafe_allow_html=True)

                        with sub_c2:
                            # УВАГА: Тут тільки ОДНА лапка в кінці!
                            st.markdown(f"**{clean_name}**")
                    else:
                        # Якщо домену немає -> просто назва
                        st.markdown(f"**{clean_name}**")
                    
                    # Решта інфо про проект (ID, лінки)
                    st.caption(f"ID: `{p_id}`")
                    if domain: st.caption(f"🌐 {domain}")
                    st.caption(f"👤 {owner_info['full_name']} | {owner_info['email']}")

                
                with c_dash:
                    if st.button("↗️", key=f"goto_{p_id}", help="Відкрити дашборд"):
                        # 1. Встановлюємо проект
                        st.session_state["current_project"] = p
                        
                        # 2. Встановлюємо ціль для меню
                        st.session_state["force_redirect_to"] = "Дашборд"
                        
                        # 3. Змінюємо ID меню, щоб воно перемалювалось з новим default_index
                        st.session_state["menu_id_counter"] = st.session_state.get("menu_id_counter", 0) + 1
                        
                        # 4. Скидаємо фокус
                        st.session_state["focus_keyword_id"] = None
                        st.rerun()
                        
                with c2:
                    curr_status = p.get('status', 'trial')
                    opts = ["trial", "active", "blocked"]
                    try: idx_s = opts.index(curr_status)
                    except: idx_s = 0
                    
                    new_status = st.selectbox("St", opts, index=idx_s, key=f"st_{p_id}", label_visibility="collapsed")
                    if new_status != curr_status:
                        update_project_field(p_id, "status", new_status)

                with c3:
                    allow_cron = p.get('allow_cron', False)
                    new_cron = st.checkbox("Дозволити", value=allow_cron, key=f"cr_{p_id}")
                    if new_cron != allow_cron:
                        update_project_field(p_id, "allow_cron", new_cron)

                with c_cnt:
                    st.markdown(f"**{k_count}**")

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
                            try:
                                supabase.table("projects").delete().eq("id", p_id).execute()
                                st.success("Видалено!")
                                time.sleep(0.5)
                                st.rerun()
                            except Exception as e:
                                st.error(str(e))
                        if st.button("❌", key=f"no_{p_id}"):
                            st.session_state[confirm_key] = False
                            st.rerun()
                
                st.markdown("<hr style='margin: 5px 0; border-top: 1px solid #eee;'>", unsafe_allow_html=True)
        
    # ========================================================
    # TAB 2: СТВОРИТИ ПРОЕКТ
    # ========================================================
    with tab_create:
        st.markdown("##### Створення нового проекту")
        
        # Використовуємо динамічний ключ для скидання полів
        rk = st.session_state["admin_reset_id"]
        
        c1, c2 = st.columns(2)
        new_name_val = c1.text_input("Назва проекту (Бренд)", key=f"new_proj_name_{rk}", placeholder="Наприклад: SkyUp")
        new_domain_val = c2.text_input("Домен", key=f"new_proj_domain_{rk}", placeholder="skyup.aero")
        
        c3, c4 = st.columns(2)
        new_industry_val = c3.text_input("Галузь (Обов'язково)", key=f"new_proj_ind_{rk}", placeholder="напр. авіаперевезення")
        
        region_options = ["Ukraine", "USA", "Europe", "Global"]
        new_region_val = c4.selectbox("Регіон", region_options, key=f"new_proj_region_{rk}")

        new_desc_val = st.text_area("Продукти/Послуги", placeholder="напр. лоукостер, квитки", height=68, key=f"new_proj_desc_{rk}")
        
        if st.button("✨ Згенерувати 10 запитів (AI)", key=f"btn_gen_{rk}"):
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
                    current_kws = st.session_state["new_proj_keywords"]
                    for kw in generated_kws:
                        current_kws.append({"keyword": kw})
                    st.session_state["new_proj_keywords"] = current_kws
                    st.success(f"Додано {len(generated_kws)} запитів!")
                else:
                    st.warning("Вебхук не повернув даних.")
            else:
                st.warning("⚠️ Заповніть: Домен, Галузь та Продукти.")

        st.divider()
        st.markdown("###### 📝 Редагування запитів перед створенням")
        
        # --- ІМПОРТ (FILE & URL) ---
        with st.expander("📥 Імпорт (Excel / URL)", expanded=False):
            st.info("💡 Завантажте файл .xlsx або вставте посилання на Google Sheet. Перша колонка має називатися **Keyword**.")
            
            import_source = st.radio("Джерело:", ["Файл (.xlsx)", "Посилання (URL)"], horizontal=True, key=f"admin_imp_src_{rk}")
            df_upload = None
            
            if import_source == "Файл (.xlsx)":
                uploaded_file = st.file_uploader("Оберіть файл Excel", type=["xlsx"], key=f"admin_kw_import_file_{rk}")
                if uploaded_file:
                    try:
                        df_upload = pd.read_excel(uploaded_file)
                    except Exception as e:
                        st.error(f"Помилка файлу: {e}")
            else:
                import_url = st.text_input("Вставте посилання (Google Sheets або CSV):", key=f"admin_kw_import_url_{rk}")
                if import_url:
                    try:
                        if "docs.google.com" in import_url:
                            match = re.search(r'/d/([a-zA-Z0-9-_]+)', import_url)
                            if match:
                                sheet_id = match.group(1)
                                csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
                                df_upload = pd.read_csv(csv_url)
                            else:
                                st.error("Не вдалося розпізнати ID Google Sheet.")
                        elif import_url.endswith(".csv"):
                            df_upload = pd.read_csv(import_url)
                        elif import_url.endswith(".xlsx"):
                            df_upload = pd.read_excel(import_url)
                        else:
                            st.warning("Пробуємо як CSV...")
                            df_upload = pd.read_csv(import_url)
                    except Exception as e:
                        if "400" in str(e): st.error("Помилка 400. Перевірте доступ (Anyone with the link).")
                        else: st.error(f"Помилка URL: {e}")

            if df_upload is not None:
                target_col = None
                cols_lower = [str(c).lower().strip() for c in df_upload.columns]
                if "keyword" in cols_lower: target_col = df_upload.columns[cols_lower.index("keyword")]
                elif "запит" in cols_lower: target_col = df_upload.columns[cols_lower.index("запит")]
                else: target_col = df_upload.columns[0]
                
                imp_kws = df_upload[target_col].dropna().astype(str).tolist()
                
                if st.button(f"Додати {len(imp_kws)} запитів", key=f"btn_add_imp_{rk}"):
                    current_kws = st.session_state["new_proj_keywords"]
                    for kw in imp_kws:
                        current_kws.append({"keyword": kw})
                    st.session_state["new_proj_keywords"] = current_kws
                    st.success("Імпортовано!")
                    st.rerun()

        # --- ТАБЛИЦЯ ЗАПИТІВ ---
        keywords_list = st.session_state["new_proj_keywords"]
        
        if not keywords_list:
            st.info("Список запитів порожній. Додайте вручну або згенеруйте.")
        else:
            for i, item in enumerate(keywords_list):
                with st.container(border=True):
                    c_num, c_txt, c_act = st.columns([0.5, 8, 1])
                    with c_num:
                        st.markdown(f"<div class='green-number'>{i+1}</div>", unsafe_allow_html=True)
                    with c_txt:
                        new_val = st.text_input("kw", value=item['keyword'], key=f"edit_kw_adm_{i}_{rk}", label_visibility="collapsed")
                        if new_val != item['keyword']:
                            st.session_state["new_proj_keywords"][i]['keyword'] = new_val
                    with c_act:
                        if st.button("🗑️", key=f"del_kw_adm_{i}_{rk}"):
                            st.session_state["new_proj_keywords"].pop(i)
                            st.rerun()

        if st.button("➕ Додати рядок", key=f"btn_plus_{rk}"):
            st.session_state["new_proj_keywords"].append({"keyword": ""})
            st.rerun()

        st.divider()
        c_st, c_cr = st.columns(2)
        new_status = c_st.selectbox("Початковий статус", ["trial", "active", "blocked"], key=f"new_proj_status_{rk}")
        new_cron = c_cr.checkbox("Дозволити автосканування одразу?", value=False, key=f"new_proj_cron_{rk}")

        if st.button("🚀 Створити проект та зберегти запити", type="primary", key=f"btn_create_{rk}"):
            final_name = new_name_val if new_name_val else new_domain_val.split('.')[0].capitalize()
            
            if new_domain_val:
                try:
                    current_user_id = st.session_state["user"].id
                    
                    new_proj_data = {
                        "user_id": current_user_id,
                        "brand_name": final_name, 
                        "domain": new_domain_val,
                        "status": new_status,
                        "allow_cron": new_cron,
                        "region": new_region_val
                    }
                    res_proj = supabase.table("projects").insert(new_proj_data).execute()
                    
                    if res_proj.data:
                        new_proj_id = res_proj.data[0]['id']
                        
                        # Whitelist Clean
                        try:
                            clean_d = new_domain_val.replace("https://", "").replace("http://", "").replace("www.", "").strip().rstrip("/")
                            supabase.table("official_assets").insert({
                                "project_id": new_proj_id, 
                                "domain_or_url": clean_d, 
                                "type": "website"
                            }).execute()
                        except: pass

                        final_kws_clean = [k['keyword'].strip() for k in keywords_list if k['keyword'].strip()]
                        
                        if final_kws_clean:
                            kws_data = [
                                {
                                    "project_id": new_proj_id, 
                                    "keyword_text": kw,
                                    "is_active": True
                                } for kw in final_kws_clean
                            ]
                            supabase.table("keywords").insert(kws_data).execute()
                        
                        # --- SUCCESS & RESET ---
                        st.session_state["new_proj_keywords"] = [] 
                        if "my_projects" in st.session_state: del st.session_state["my_projects"]
                        
                        # Змінюємо ключ, щоб очистити інпути
                        st.session_state["admin_reset_id"] += 1
                        
                        st.success(f"✅ Проект '{final_name}' успішно створено!")
                        time.sleep(2)
                        st.rerun()
                except Exception as e:
                    st.error(f"Помилка створення: {e}")
            else:
                st.warning("Домен обов'язковий.")

    # ========================================================
    # TAB 3: КОРИСТУВАЧІ ТА ПРАВА (NEW LINE PROJECTS)
    # ========================================================
    with tab_users:
        st.markdown("##### 👥 База користувачів")

        uf1, uf2 = st.columns(2)
        with uf1:
            u_search = st.text_input("🔍 Пошук користувача", placeholder="Ім'я або email")
        with uf2:
            role_filter = st.multiselect("Роль", ["user", "admin", "super_admin"], default=[])

        if users_data:
            proj_df = pd.DataFrame(projects_data)
            
            user_table_data = []
            for u in users_data:
                full_name = f"{u.get('first_name', '')} {u.get('last_name', '')}".strip()
                email = u.get('email', '')
                
                search_target = f"{full_name} {email}".lower()
                if u_search and u_search.lower() not in search_target: continue
                if role_filter and u.get('role', 'user') not in role_filter: continue

                user_projs = []
                if not proj_df.empty and 'user_id' in proj_df.columns:
                    my_projs = proj_df[proj_df['user_id'] == u['id']]
                    for _, p_row in my_projs.iterrows():
                        p_nm = p_row.get('brand_name') or p_row.get('project_name') or 'NoName'
                        p_dt = p_row.get('created_at', '')[:10]
                        user_projs.append(f"{p_nm} ({p_dt})")
                
                # 🔥 FIX: Новий рядок
                projs_str = "\n".join(user_projs) if user_projs else "-"

                user_table_data.append({
                    "id": u['id'],
                    "Ім'я": full_name,
                    "Email": email,
                    "Роль": u.get('role', 'user'),
                    "Проекти": projs_str, 
                    "Зареєстрований": u.get('created_at', '')[:10]
                })
            
            df_users_view = pd.DataFrame(user_table_data)
            
            if not df_users_view.empty:
                df_users_view.index = np.arange(1, len(df_users_view) + 1)
                
                edited_users = st.data_editor(
                    df_users_view,
                    column_config={
                        "id": st.column_config.TextColumn("User ID", disabled=True, width="small"),
                        "Email": st.column_config.TextColumn("Email", disabled=True),
                        "Ім'я": st.column_config.TextColumn("Ім'я", disabled=True),
                        "Проекти": st.column_config.TextColumn("Проекти (Дата)", disabled=True, width="large"),
                        "Зареєстрований": st.column_config.TextColumn("Дата реєстрації", disabled=True),
                        "Роль": st.column_config.SelectboxColumn("Роль", options=["user", "admin", "super_admin"], required=True)
                    },
                    use_container_width=True,
                    key="admin_users_final_v4"
                )

                if st.button("💾 Зберегти зміни прав"):
                    try:
                        changes_count = 0
                        updated_rows = edited_users.to_dict('index') 
                        
                        for idx, row in updated_rows.items():
                            uid = row['id']
                            new_role = row['Роль']
                            
                            old_user = next((u for u in users_data if u['id'] == uid), None)
                            if old_user and old_user.get('role') != new_role:
                                supabase.table("profiles").update({"role": new_role}).eq("id", uid).execute()
                                changes_count += 1
                        
                        if changes_count > 0:
                            st.success(f"Успішно оновлено {changes_count} користувачів!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.info("Змін не виявлено.")
                            
                    except Exception as e:
                        st.error(f"Помилка збереження: {e}")

                st.divider()
                st.markdown("##### 📈 Динаміка реєстрацій")
                
                df_chart = pd.DataFrame(users_data)
                if 'created_at' in df_chart.columns:
                    df_chart['date'] = pd.to_datetime(df_chart['created_at']).dt.date
                    from datetime import timedelta
                    time_filter = st.selectbox("Період", ["Останні 7 днів", "Останні 30 днів", "Останні 90 днів", "Весь час"], index=1)
                    
                    today = pd.to_datetime("today").date()
                    if "7" in time_filter: start_date = today - timedelta(days=7)
                    elif "30" in time_filter: start_date = today - timedelta(days=30)
                    elif "90" in time_filter: start_date = today - timedelta(days=90)
                    else: start_date = df_chart['date'].min()
                    
                    df_chart_filtered = df_chart[df_chart['date'] >= start_date]
                    reg_counts = df_chart_filtered.groupby('date').size().reset_index(name='count')
                    
                    if not reg_counts.empty:
                        fig = px.bar(reg_counts, x='date', y='count', labels={'date': 'Дата', 'count': 'Нових користувачів'})
                        fig.update_layout(height=300)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("Немає реєстрацій за цей період.")
            else:
                st.warning("Користувачів не знайдено.")
        else:
            st.warning("База користувачів пуста.")


def show_chat_page():
    """
    Сторінка AI-асистента (GPT-Visibility).
    ВЕРСІЯ: ADDED CONTEXT (SOURCES, BRAND, USER NAME).
    1. Передає official_sources (список доменів з бази).
    2. Передає user_name (з метаданих або email).
    3. Передає target_brand.
    """
    import requests
    import streamlit as st

    # --- КОНФІГУРАЦІЯ ---
    # Перевірка наявності URL
    if 'N8N_CHAT_WEBHOOK' not in globals():
        target_url = st.secrets.get("N8N_CHAT_WEBHOOK", "")
        if not target_url:
            st.error("🚨 Не задано посилання N8N_CHAT_WEBHOOK.")
            return
    else:
        target_url = N8N_CHAT_WEBHOOK

    # Підключення до бази (для отримання джерел)
    if 'supabase' in st.session_state:
        supabase = st.session_state['supabase']
    elif 'supabase' in globals():
        supabase = globals()['supabase']
    else:
        st.error("🚨 Змінна 'supabase' не знайдена.")
        return

    headers = {
        "virshi-auth": "hi@virshi.ai2025" 
    }

    st.title("🤖 GPT-Visibility Assistant")
    
    # 1. Отримуємо контекст користувача та проекту
    user = st.session_state.get("user")
    role = st.session_state.get("role", "user") 
    proj = st.session_state.get("current_project", {})
    
    if not proj:
        st.warning("⚠️ Будь ласка, оберіть проект у меню зліва.")
        return

    # 2. Логіка отримання імені користувача
    user_name = "Користувач"
    if user:
        # Спроба дістати ім'я з метаданих Supabase, інакше email
        meta = getattr(user, "user_metadata", {})
        user_name = meta.get("full_name") or meta.get("name") or user.email.split("@")[0]

    # 3. Логіка отримання офіційних джерел (Whitelist)
    official_sources_list = []
    try:
        # Робимо запит до бази, щоб агент знав "білий список"
        assets_resp = supabase.table("official_assets")\
            .select("domain_or_url")\
            .eq("project_id", proj.get("id"))\
            .execute()
        
        if assets_resp.data:
            official_sources_list = [item["domain_or_url"] for item in assets_resp.data]
    except Exception:
        official_sources_list = [] # Якщо помилка, просто пустий список

    # 4. Ініціалізація історії
    if "messages" not in st.session_state:
        brand_name = proj.get('brand_name', 'вашого бренду')
        welcome_text = f"Привіт, {user_name}! Я аналітик проекту **{brand_name}**. Готовий допомогти з аналізом видимості та конкурентів."
        st.session_state["messages"] = [
            {"role": "assistant", "content": welcome_text}
        ]

    # 5. Відображення історії
    for msg in st.session_state["messages"]:
        avatar_icon = "👤" if msg["role"] == "user" else "🤖"
        with st.chat_message(msg["role"], avatar=avatar_icon):
            st.markdown(msg["content"])

    # 6. Обробка вводу
    if prompt := st.chat_input("Напишіть ваше запитання..."):
        
        # A. Користувач
        st.session_state["messages"].append({"role": "user", "content": prompt})
        with st.chat_message("user", avatar="👤"):
            st.markdown(prompt)

        # B. Відправка на n8n
        with st.chat_message("assistant", avatar="🤖"):
            message_placeholder = st.empty()
            
            with st.spinner("Аналізую дані..."):
                try:
                    # --- 🔥 РОЗШИРЕНИЙ PAYLOAD ---
                    payload = {
                        "query": prompt,
                        
                        # Користувач
                        "user_id": user.id if user else "guest",
                        "user_email": user.email if user else None,
                        "user_name": user_name,  # <--- Ім'я
                        "role": role,
                        
                        # Проект
                        "project_id": proj.get("id"),
                        "project_name": proj.get("brand_name"),
                        "target_brand": proj.get("brand_name"), # <--- Цільовий бренд
                        "domain": proj.get("domain"),
                        "status": proj.get("status"),
                        
                        # Контекст
                        "official_sources": official_sources_list # <--- Список джерел
                    }

                    response = requests.post(
                        target_url, 
                        json=payload, 
                        headers=headers, 
                        timeout=60
                    )

                    if response.status_code == 200:
                        data = response.json()
                        bot_reply = data.get("output") or data.get("answer") or data.get("text")
                        
                        if isinstance(bot_reply, dict):
                            bot_reply = str(bot_reply)
                        
                        if not bot_reply:
                            bot_reply = "⚠️ Отримана пуста відповідь від AI."
                            
                    elif response.status_code == 403:
                        bot_reply = "⛔ Помилка 403: Доступ заборонено. Перевірте Header Name 'virshi-auth' в n8n."
                    elif response.status_code == 404:
                        bot_reply = f"⚠️ Помилка 404 (Not Found).\n\n1. Перевірте метод **POST** в n8n.\n2. Перевірте, що Workflow **Active**."
                    else:
                        bot_reply = f"⚠️ Помилка сервера: {response.status_code} - {response.text}"

                except Exception as e:
                    bot_reply = f"⚠️ Помилка з'єднання: {e}"

                # C. Вивід
                message_placeholder.markdown(bot_reply)
        
        # D. Збереження
        st.session_state["messages"].append({"role": "assistant", "content": bot_reply})
        
            
def main():
    # 1. Session Check
    if 'check_session' in globals():
        check_session()

    # 2. If not logged in -> Show Auth Page
    if not st.session_state.get("user"):
        # Переконайтеся, що show_auth_page визначена
        if 'show_auth_page' in globals():
            show_auth_page()
        else:
            st.error("Функція авторизації не знайдена.")
        return

    # 3. ОТРИМАННЯ ДАНИХ ПРОЕКТУ
    if not st.session_state.get("current_project"):
        try:
            user_id = st.session_state["user"].id
            resp = supabase.table("projects").select("*").eq("user_id", user_id).execute()
            if resp.data:
                # Беремо перший знайдений проект
                st.session_state["current_project"] = resp.data[0]
                st.rerun()
        except Exception:
            pass

    # 4. ЛОГІКА ONBOARDING
    # Якщо проекту немає і це не адмін
    user_role = st.session_state.get("role", "user")
    
    if st.session_state.get("current_project") is None and user_role not in ["admin", "super_admin"]:
        with st.sidebar:
            # Логотип
            st.image("https://raw.githubusercontent.com/virshi-ai/image/refs/heads/main/logo-removebg-preview.png", width=150)
            if st.button("Вийти"):
                logout()
        
        # Запуск майстра
        if 'onboarding_wizard' in globals():
            onboarding_wizard()
        else:
            st.error("Onboarding Wizard not found.")
    
    # 5. ОСНОВНИЙ ДОДАТОК
    else:
        # Виклик меню
        page = sidebar_menu()

        # Роутинг сторінок
        if page == "Дашборд":
            show_dashboard()
        elif page == "Перелік запитів":
            show_keywords_page()
        elif page == "Джерела":
            show_sources_page()
        elif page == "Конкуренти":
            # Якщо окремої сторінки немає, можна використати частину дашборду або заглушку
            if 'show_competitors_page' in globals():
                show_competitors_page()
            else:
                st.info("Розділ у розробці (див. Дашборд).")
        elif page == "Рекомендації":
            show_recommendations_page()
            
        # --- НОВІ СТОРІНКИ ---
        elif page == "Історія сканувань":
            if 'show_history_page' in globals(): show_history_page()
            else: st.warning("Функція show_history_page не знайдена.")
            
        elif page == "Звіти":
            show_reports_page()
            
        elif page == "FAQ":
            if 'show_faq_page' in globals(): show_faq_page()
            else: st.warning("Функція show_faq_page не знайдена.")
        # ---------------------

        elif page == "GPT-Visibility":
            show_chat_page()
            
        elif page == "Адмін":
            if user_role in ["admin", "super_admin"]:
                show_admin_page()
            else:
                st.error("Доступ заборонено.")

if __name__ == "__main__":
    main()
