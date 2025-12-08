import time
from datetime import datetime, timedelta, date
import plotly.express as px 
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

    /* Додати до існуючих стилів */
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
    .metric-card-small {
        background-color: #F0F2F6;
        border-radius: 6px;
        padding: 10px;
        text-align: center;
    }
    .metric-value {
        font-size: 18px;
        font-weight: bold;
        color: #8041F6;
    }
    .metric-label {
        font-size: 12px;
        color: #666;
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
    # 👇 ДОДАЙТЕ ЦЕЙ БЛОК 👇
    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }
    # ----------------------
    
    try:
        user_email = st.session_state["user"].email if st.session_state.get("user") else None
        
        if isinstance(keywords, str):
            keywords = [keywords]

        # Якщо моделі не обрані або пусті, беремо Perplexity
        if not models:
            models = ["Perplexity"]

        success_count = 0

        # Отримуємо офіційні джерела
        try:
            assets_resp = supabase.table("official_assets")\
                .select("domain_or_url")\
                .eq("project_id", project_id)\
                .execute()
            official_assets = [item["domain_or_url"] for item in assets_resp.data] if assets_resp.data else []
        except Exception as e:
            print(f"Error fetching assets: {e}")
            official_assets = []

        # 🔄 ЦИКЛ по моделях
        for ui_model_name in models:
            # Конвертуємо красиву назву в технічний ID для n8n
            # Якщо назви немає в словнику, використовуємо як є
            tech_model_id = MODEL_MAPPING.get(ui_model_name, ui_model_name)

            payload = {
                "project_id": project_id,
                "keywords": keywords, 
                "brand_name": brand_name,
                "user_email": user_email,
                "provider": tech_model_id, # <--- Відправляємо технічний ID (gpt-4o)
                "models": [tech_model_id],
                "official_assets": official_assets
            }
            
            try:
                response = requests.post(N8N_ANALYZE_URL, json=payload, timeout=5)
                if response.status_code == 200:
                    success_count += 1
            except Exception as inner_e:
                st.error(f"Не вдалося запустити {ui_model_name}: {inner_e}")

        return success_count > 0
            
    except Exception as e:
        st.error(f"Помилка з'єднання з n8n: {e}")
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
    import plotly.graph_objects as go
    from datetime import datetime, timedelta, time as dt_time
    
    proj = st.session_state.get("current_project", {})
    if not proj:
        st.info("Спочатку створіть проект.")
        return

    # --- 1. ВИЗНАЧЕННЯ ПЕРІОДУ (SMART DATE RANGE) ---
    
    # Спробуємо знайти дату найпершого сканування, щоб виставити її як старт
    try:
        first_scan = supabase.table("scan_results")\
            .select("created_at")\
            .eq("project_id", proj["id"])\
            .order("created_at", desc=False)\
            .limit(1)\
            .execute()
        
        if first_scan.data:
            # Парсимо дату першого сканування
            min_date_str = first_scan.data[0]['created_at']
            min_date = datetime.fromisoformat(min_date_str.replace('Z', '+00:00')).date()
        else:
            # Якщо даних немає, ставимо "сьогодні - 30 днів"
            min_date = datetime.now().date() - timedelta(days=30)
    except:
        min_date = datetime.now().date() - timedelta(days=30)

    today = datetime.now().date()

    # ВЕРХНЯ ПАНЕЛЬ
    c_title, c_date = st.columns([3, 1])
    with c_title:
        st.title(f"📊 Дашборд: {proj.get('brand_name')}")
    
    with c_date:
        # По дефолту беремо від min_date до today (ВЕСЬ ПЕРІОД)
        date_range = st.date_input(
            "Період аналізу:",
            value=(min_date, today),
            min_value=min_date,
            max_value=today,
            format="DD.MM.YYYY"
        )

    st.markdown("---")

    # Валідація дат
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    elif isinstance(date_range, tuple) and len(date_range) == 1:
        # Якщо користувач тільки клацнув на першу дату
        start_date = date_range[0]
        end_date = today
    else:
        start_date = min_date
        end_date = today

    # --- 2. ЗАВАНТАЖЕННЯ ДАНИХ ---
    try:
        # Перетворюємо дати в ISO формат для Supabase (початок дня і кінець дня)
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
            st.warning(f"🔍 За період з {start_date.strftime('%d.%m')} по {end_date.strftime('%d.%m')} даних не знайдено.")
            st.info("👉 Перейдіть у вкладку **'Перелік запитів'** та запустіть нове сканування, або оберіть ширший діапазон дат.")
            return # Зупиняємо виконання, щоб не показувати пусті графіки

        # B. Згадки
        mentions_resp = supabase.table("brand_mentions")\
            .select("*")\
            .in_("scan_result_id", scan_ids)\
            .execute()
        df_mentions = pd.DataFrame(mentions_resp.data)

        # C. Джерела
        sources_resp = supabase.table("extracted_sources")\
            .select("*")\
            .in_("scan_result_id", scan_ids)\
            .execute()
        df_sources = pd.DataFrame(sources_resp.data)

        # D. Ключові слова
        keywords_resp = supabase.table("keywords")\
            .select("id, keyword_text")\
            .eq("project_id", proj["id"])\
            .execute()
        kw_map = {k['id']: k['keyword_text'] for k in keywords_resp.data}

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

    # 3. Sentiment
    my_brand_rows = df_mentions[df_mentions['is_my_brand'] == True].copy() if not df_mentions.empty else pd.DataFrame()
    
    def calc_sent_score(s):
        if s == 'Позитивний': return 100
        if s == 'Негативний': return 0
        return 50 
    
    avg_sentiment = 0
    if not my_brand_rows.empty:
        my_brand_rows['score'] = my_brand_rows['sentiment_score'].apply(calc_sent_score)
        avg_sentiment = my_brand_rows['score'].mean()

    # 4. Позиція
    found_rows = my_brand_rows[my_brand_rows['rank_position'].notnull()] if not my_brand_rows.empty else pd.DataFrame()
    avg_pos = found_rows['rank_position'].mean() if not found_rows.empty else 0

    # 5. Присутність
    total_scans_count = len(scan_ids)
    scans_with_me = found_rows['scan_result_id'].nunique() if not found_rows.empty else 0
    visibility_rate = (scans_with_me / total_scans_count * 100) if total_scans_count > 0 else 0.0

    # --- 4. ВІЗУАЛІЗАЦІЯ (СТИЛЬ VIRSHI) ---
    
    def make_donut(value, label, color="#00C896"):
        fig = go.Figure(data=[go.Pie(
            values=[value, 100-value],
            hole=.75,
            marker_colors=[color, "#EEF0F2"],
            textinfo='none',
            hoverinfo='none'
        )])
        fig.update_layout(
            showlegend=False,
            margin=dict(t=0, b=0, l=0, r=0),
            height=120,
            annotations=[dict(text=f"{value:.1f}%", x=0.5, y=0.5, font_size=20, showarrow=False, font_weight='bold')]
        )
        return fig

    st.markdown("""
    <style>
        .dash-card {
            background-color: white;
            border: 1px solid #E0E0E0;
            border-radius: 10px;
            padding: 20px;
            text-align: center;
            box-shadow: 0 2px 4px rgba(0,0,0,0.02);
            height: 280px;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }
        .dash-title {
            font-size: 12px;
            text-transform: uppercase;
            color: #888;
            font-weight: 600;
            margin-bottom: 10px;
        }
    </style>
    """, unsafe_allow_html=True)

    # --- РЯДОК 1 ---
    r1_c1, r1_c2, r1_c3 = st.columns(3)

    with r1_c1:
        with st.container(border=True):
            st.markdown("<div class='dash-title'>ЧАСТКА ГОЛОСУ (SOV)</div>", unsafe_allow_html=True)
            st.plotly_chart(make_donut(sov, "SOV"), use_container_width=True, key="d_sov")
            st.caption(f"Ви: {int(my_mentions)} | Всього: {int(total_mentions)}")

    with r1_c2:
        with st.container(border=True):
            st.markdown("<div class='dash-title'>% ОФІЦІЙНИХ ДЖЕРЕЛ</div>", unsafe_allow_html=True)
            st.plotly_chart(make_donut(official_pct, "OFF", color="#36A2EB"), use_container_width=True, key="d_off")
            st.caption(f"Офіційних: {official_sources} | Всього: {total_sources}")

    with r1_c3:
        with st.container(border=True):
            st.markdown("<div class='dash-title'>ЗАГАЛЬНИЙ НАСТРІЙ</div>", unsafe_allow_html=True)
            st.plotly_chart(make_donut(avg_sentiment, "Sent", color="#FFCE56"), use_container_width=True, key="d_sent")
            
            sent_text = "Нейтральний"
            if avg_sentiment > 60: sent_text = "Позитивний"
            if avg_sentiment < 40: sent_text = "Негативний"
            if avg_sentiment == 0 and my_brand_rows.empty: sent_text = "Даних немає"
            
            st.markdown(f"<div style='text-align:center; font-weight:bold;'>{sent_text}</div>", unsafe_allow_html=True)

    # --- РЯДОК 2 ---
    r2_c1, r2_c2, r2_c3 = st.columns(3)

    with r2_c1:
        with st.container(border=True):
            st.markdown("<div class='dash-title'>СЕРЕДНЯ ПОЗИЦІЯ</div>", unsafe_allow_html=True)
            val_display = f"{avg_pos:.1f}" if avg_pos > 0 else "-"
            st.markdown(f"<div style='text-align:center; font-size: 48px; font-weight: bold; color: #00C896; margin-top: 30px;'>{val_display}</div>", unsafe_allow_html=True)
            
            # Gauge Chart
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
            st.plotly_chart(make_donut(visibility_rate, "Vis", color="#9966FF"), use_container_width=True, key="d_vis")
            st.caption(f"Знайдено у {scans_with_me} з {total_scans_count} сканувань")

    with r2_c3:
        # Метрика: Кількість унікальних запитів
        unique_kws = len(set(scan_ids)) # Це спрощено, краще брати keyword_id
        # Але краще: Доля посилань на домен
        domain_mentions = len(df_sources[df_sources['domain'].str.contains(proj.get('domain', 'MISSING'), na=False, case=False)])
        domain_pct = (domain_mentions / total_sources * 100) if total_sources > 0 else 0
        
        with st.container(border=True):
            st.markdown("<div class='dash-title'>ЗГАДКИ ДОМЕНУ</div>", unsafe_allow_html=True)
            st.plotly_chart(make_donut(domain_pct, "Dom", color="#FF9F40"), use_container_width=True, key="d_dom")
            st.caption(f"{domain_mentions} прямих посилань")

    st.markdown("---")

    # --- 5. ТАБЛИЦЯ ЗАПИТІВ (ДИНАМІЧНА) ---
    st.subheader("📋 Деталізація за період")
    
    # Формуємо таблицю на основі filtered scans
    # Беремо найсвіжіший скан для кожного keyword_id В МЕЖАХ ОБРАНОГО ПЕРІОДУ
    latest_scans_df = pd.DataFrame(scans_query.data)
    if not latest_scans_df.empty:
        latest_scans_df = latest_scans_df.sort_values('created_at', ascending=False).drop_duplicates('keyword_id')
        
        table_rows = []
        for index, row in latest_scans_df.iterrows():
            kw_text = kw_map.get(row['keyword_id'], "Видалений запит")
            
            # Дані конкретного сканування
            scan_mentions = df_mentions[df_mentions['scan_result_id'] == row['id']]
            my_mention = scan_mentions[scan_mentions['is_my_brand'] == True]
            
            if not my_mention.empty:
                pos = my_mention.iloc[0]['rank_position']
                sent = my_mention.iloc[0]['sentiment_score']
                is_present = True
            else:
                pos = None # Для сортування краще None
                sent = "Не знайдено"
                is_present = False
                
            table_rows.append({
                "Запит": kw_text,
                "Дата": datetime.fromisoformat(row['created_at']).strftime("%d.%m.%Y"),
                "Позиція": pos if pos else "-",
                "Тональність": sent,
                "Знайдено?": is_present
            })
            
        df_table = pd.DataFrame(table_rows)
        
        st.dataframe(
            df_table,
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
    Відображає детальну аналітику по запиту з KPI картками у стилі Virshi (Green/White).
    """
    import pandas as pd
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

    # Локальний мапінг
    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }

    # --- 1. ОТРИМАННЯ ДАНИХ ЗАПИТУ ---
    try:
        kw_resp = supabase.table("keywords").select("*").eq("id", kw_id).execute()
        if not kw_resp.data:
            st.error("Запит не знайдено.")
            if st.button("⬅ Назад"):
                st.session_state["focus_keyword_id"] = None
                st.rerun()
            return
        
        keyword_record = kw_resp.data[0]
        keyword_text = keyword_record["keyword_text"]
        project_id = keyword_record["project_id"]
    except Exception as e:
        st.error(f"Помилка БД: {e}")
        return

    # --- 2. HEADER ---
    col_back, col_title = st.columns([1, 10])
    with col_back:
        if st.button("⬅", key="back_main", help="Назад до списку"):
            st.session_state["focus_keyword_id"] = None
            st.rerun()
    
    with col_title:
        st.markdown(f"<h2 style='margin-top: -10px;'>🔍 {keyword_text}</h2>", unsafe_allow_html=True)

    # --- 3. БЛОК УПРАВЛІННЯ (Прихований в експандер для чистоти) ---
    with st.expander("⚙️ Налаштування та Нове сканування", expanded=False):
        c1, c2 = st.columns(2)
        with c1:
            new_text = st.text_input("Редагувати запит", value=keyword_text, key="edit_kw_input")
            if st.button("💾 Зберегти", key="save_kw_btn"):
                if new_text and new_text != keyword_text:
                    supabase.table("keywords").update({"keyword_text": new_text}).eq("id", kw_id).execute()
                    st.success("Збережено!")
                    st.rerun()

        with c2:
            model_choices = list(MODEL_MAPPING.keys())
            selected_models_ui = st.multiselect("Запустити пересканування:", model_choices, default=["Perplexity"], key="rescan_models_select")
            
            if st.button("🚀 Сканувати", key="rescan_btn"):
                if selected_models_ui:
                    proj = st.session_state.get("current_project", {})
                    brand_name = proj.get("brand_name", "MyBrand")
                    with st.spinner(f"Запускаємо {', '.join(selected_models_ui)}..."):
                        success = n8n_trigger_analysis(project_id, [new_text], brand_name, models=selected_models_ui)
                        if success:
                            st.success("Задачу відправлено! Оновіть сторінку за хвилину.")
                else:
                    st.warning("Оберіть хоча б одну ЛЛМ.")

    st.markdown("<br>", unsafe_allow_html=True)

    # --- 4. ЗАВАНТАЖЕННЯ ІСТОРІЇ ---
    try:
        scans_data = (
            supabase.table("scan_results")
            .select("*")
            .eq("keyword_id", kw_id)
            .order("created_at", desc=True)
            .execute()
            .data
        )
    except Exception as e:
        st.error(f"Не вдалося завантажити історію: {e}")
        scans_data = []

    if not scans_data:
        st.info("📭 Для цього запиту ще немає результатів.")
        return

    # --- 5. ВКЛАДКИ ПО МОДЕЛЯХ ---
    tabs = st.tabs(list(MODEL_MAPPING.keys()))

    for tab, ui_model_name in zip(tabs, MODEL_MAPPING.keys()):
        with tab:
            tech_model_id = MODEL_MAPPING[ui_model_name]
            model_scans = [s for s in scans_data if tech_model_id in (s.get("provider") or "").lower()]
            
            if not model_scans:
                st.write(f"📉 Даних від **{ui_model_name}** ще немає.")
                continue

            # Вибір дати
            history_options = {s["created_at"][:16].replace("T", " "): s for s in model_scans}
            col_date, _ = st.columns([2, 4])
            with col_date:
                selected_time = st.selectbox(
                    f"📅 Дата аналізу ({ui_model_name}):", 
                    list(history_options.keys()),
                    key=f"hist_sel_{tech_model_id}"
                )
            
            current_scan = history_options[selected_time]
            scan_id = current_scan["id"]

            # =========================================================
            # 👇 НОВИЙ UI: КАРТКИ KPI (Як на макеті)
            # =========================================================
            
            # 1. Завантажуємо згадки
            try:
                mentions_kpi = supabase.table("brand_mentions").select("*").eq("scan_result_id", scan_id).execute().data
            except:
                mentions_kpi = []

            # 2. Розрахунок метрик
            total_market_mentions = sum(item.get("mention_count", 0) for item in mentions_kpi) if mentions_kpi else 0
            my_brand_data = next((item for item in mentions_kpi if item.get("is_my_brand") is True), None)

            if my_brand_data:
                val_count = my_brand_data.get("mention_count", 0)
                val_sentiment = my_brand_data.get("sentiment_score", "Нейтральний")
                val_position = my_brand_data.get("rank_position", 0)
                val_sov = (val_count / total_market_mentions * 100) if total_market_mentions > 0 else 0
            else:
                val_count = 0
                val_sentiment = "Не згадано"
                val_position = 0 # Якщо не знайдено
                val_sov = 0

            # Колір для сентименту
            sent_color = "#333"
            if val_sentiment == "Позитивний": sent_color = "#00C896"
            elif val_sentiment == "Негативний": sent_color = "#FF4B4B"
            elif val_sentiment == "Не згадано": sent_color = "#999"

            # 3. HTML/CSS Стилізація (Зелений контур, Тіні, Шрифт)
            st.markdown(f"""
            <style>
                .virshi-kpi-container {{
                    display: grid;
                    grid-template-columns: repeat(4, 1fr);
                    gap: 15px;
                    margin-bottom: 25px;
                    font-family: 'Source Sans Pro', sans-serif;
                }}
                .virshi-card {{
                    background-color: white;
                    border: 1px solid #E0E0E0;
                    border-top: 4px solid #00C896; /* Зелений верхній бордюр */
                    border-radius: 8px;
                    padding: 20px 15px;
                    text-align: center;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.04);
                    transition: transform 0.2s;
                }}
                .virshi-card:hover {{
                    transform: translateY(-2px);
                    box-shadow: 0 6px 12px rgba(0,0,0,0.08);
                }}
                .virshi-label {{
                    color: #888;
                    font-size: 11px;
                    text-transform: uppercase;
                    letter-spacing: 1px;
                    font-weight: 600;
                    margin-bottom: 10px;
                }}
                .virshi-value {{
                    color: #111;
                    font-size: 28px;
                    font-weight: 700;
                    line-height: 1.2;
                }}
                .virshi-sub {{
                    font-size: 14px;
                    color: {sent_color};
                    font-weight: 600;
                }}
                /* Мобільна адаптація */
                @media (max-width: 768px) {{
                    .virshi-kpi-container {{ grid-template-columns: repeat(2, 1fr); }}
                }}
            </style>

            <div class="virshi-kpi-container">
                <div class="virshi-card">
                    <div class="virshi-label">Частка Голосу (SOV)</div>
                    <div class="virshi-value">{val_sov:.1f}%</div>
                </div>
                <div class="virshi-card">
                    <div class="virshi-label">Згадок Бренду</div>
                    <div class="virshi-value">{val_count}</div>
                </div>
                <div class="virshi-card">
                    <div class="virshi-label">Тональність</div>
                    <div class="virshi-value virshi-sub">{val_sentiment}</div>
                </div>
                <div class="virshi-card">
                    <div class="virshi-label">Позиція у списку</div>
                    <div class="virshi-value">{val_position if val_position > 0 else "-"}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

            # =========================================================
            # ВІДПОВІДЬ ШІ (Зелений заголовок)
            # =========================================================
            raw_text = current_scan.get("raw_response", "")
            
            st.markdown("#### 📝 Відповідь ЛЛМ")
            with st.container(border=True):
                if raw_text:
                    my_brand = st.session_state.get("current_project", {}).get("brand_name", "")
                    # Підсвітка бренду зеленим жирним
                    highlighted_text = raw_text.replace(my_brand, f"<span style='color:#00C896; font-weight:bold;'>{my_brand}</span>")
                    # Заміна markdown bold на зелений bold, якщо треба, або просто рендер
                    st.markdown(highlighted_text, unsafe_allow_html=True)
                else:
                    st.caption("Текст відповіді не збережено.")

            st.markdown("<br>", unsafe_allow_html=True)

            # =========================================================
            # ТАБЛИЦІ (Clean Table Style)
            # =========================================================
            
            # 1. БРЕНДИ
            st.markdown("#### 📊 Конкурентний аналіз")
            if mentions_kpi:
                df_brands = pd.DataFrame(mentions_kpi)
                df_brands = df_brands.sort_values(by="rank_position", ascending=True)
                
                cols = ["rank_position", "brand_name", "sentiment_score", "mention_count", "is_my_brand"]
                avail_cols = [c for c in cols if c in df_brands.columns]
                show_df = df_brands[avail_cols].copy()
                
                rename_map = {
                    "rank_position": "Позиція", 
                    "brand_name": "Бренд", 
                    "sentiment_score": "Настрій", 
                    "mention_count": "Згадок", 
                    "is_my_brand": "Це ми?"
                }
                show_df.rename(columns=rename_map, inplace=True)
                
                # Додаємо галочку
                if "Це ми?" in show_df.columns:
                    show_df["Це ми?"] = show_df["Це ми?"].apply(lambda x: "✅" if x else "")

                st.dataframe(
                    show_df, 
                    use_container_width=True, 
                    hide_index=True,
                    column_config={
                        "Позиція": st.column_config.NumberColumn("Позиція", format="%d"),
                        "Згадок": st.column_config.ProgressColumn("Згадок", format="%d", min_value=0, max_value=int(show_df["Згадок"].max())),
                    }
                )
            else:
                st.info("Брендів не знайдено.")

            st.markdown("<br>", unsafe_allow_html=True)

            # 2. ДЖЕРЕЛА
            st.markdown("#### 🔗 Цитовані джерела")
            try:
                sources = (
                    supabase.table("extracted_sources")
                    .select("*")
                    .eq("scan_result_id", scan_id)
                    .execute()
                    .data
                )
                if sources:
                    df_src = pd.DataFrame(sources)
                    s_cols = ["domain", "url", "is_official"]
                    s_avail = [c for c in s_cols if c in df_src.columns]
                    show_src = df_src[s_avail].copy()
                    
                    show_src.rename(columns={"domain": "Домен", "url": "Посилання", "is_official": "Офіційне?"}, inplace=True)
                    
                    if "Офіційне?" in show_src.columns:
                        show_src["Офіційне?"] = show_src["Офіційне?"].apply(lambda x: "✅" if x else "")

                    st.dataframe(
                        show_src, 
                        use_container_width=True, 
                        hide_index=True,
                        column_config={
                            "Посилання": st.column_config.LinkColumn("Посилання")
                        }
                    )
                else:
                    st.caption("Джерел не знайдено.")
            except Exception as e:
                st.error(f"Помилка джерел: {e}")
def show_keywords_page():
    """
    Сторінка списку запитів з розширеним сортуванням та масовим вибором.
    """
    import pandas as pd
    import streamlit as st
    from datetime import datetime
    import time # Додаємо імпорт часу

    # Локальний мапінг (щоб уникнути помилок NameError)
    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }

    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект в онбордингу.")
        return

    if st.session_state.get("focus_keyword_id"):
        show_keyword_details(st.session_state["focus_keyword_id"])
        return

    st.title("📋 Перелік запитів")

    # --- 1. ФОРМА ДОДАВАННЯ ---
    with st.expander("➕ Додати новий запит", expanded=False):
        with st.form("add_keyword_form"):
            new_kw = st.text_input("Введіть запит")
            model_choices = list(MODEL_MAPPING.keys())
            selected_models = st.multiselect("Оберіть ЛЛМ:", model_choices, default=["Perplexity"])
            
            if st.form_submit_button("Додати та Просканувати"):
                if new_kw:
                    try:
                        res = supabase.table("keywords").insert({
                            "project_id": proj["id"], "keyword_text": new_kw, "is_active": True
                        }).execute()
                        if res.data:
                            n8n_trigger_analysis(proj["id"], [new_kw], proj.get("brand_name"), models=selected_models)
                            st.success(f"Запит '{new_kw}' додано.")
                            time.sleep(1)
                            st.rerun()
                    except Exception as e:
                        st.error(f"Помилка: {e}")

    st.divider()
    
    # --- 2. ОТРИМАННЯ ТА ОБРОБКА ДАНИХ ---
    try:
        # А. Отримуємо всі запити
        keywords = supabase.table("keywords").select("*").eq("project_id", proj["id"]).execute().data
        
        # Б. Отримуємо останні дати сканувань
        last_scans_resp = supabase.table("scan_results")\
            .select("keyword_id, created_at")\
            .eq("project_id", proj["id"])\
            .order("created_at", desc=True)\
            .execute()
            
        # Словник {keyword_id: "2023-12-08T14:00..."}
        last_scan_map = {}
        if last_scans_resp.data:
            for s in last_scans_resp.data:
                kw_id = s['keyword_id']
                if kw_id not in last_scan_map:
                    last_scan_map[kw_id] = s['created_at']
        
        # В. Збагачуємо список запитів датою сканування для сортування
        for k in keywords:
            k['last_scan_date'] = last_scan_map.get(k['id'], "1970-01-01T00:00:00+00:00") # Дефолтна стара дата

    except Exception as e:
        st.error(f"Помилка завантаження: {e}")
        keywords = []

    if not keywords:
        st.info("Запити відсутні.")
        return

    # --- 3. ПАНЕЛЬ ІНСТРУМЕНТІВ (Сортування та Дії) ---
    col_tools_1, col_tools_2, col_tools_3 = st.columns([1.5, 1.5, 1])
    
    with col_tools_1:
        # Сортування
        sort_option = st.selectbox(
            "Сортувати за:", 
            ["Найновіші (Додані)", "Найстаріші (Додані)", "Нещодавно проскановані", "Давно не скановані"],
            label_visibility="collapsed"
        )

    # Логіка сортування Python
    if sort_option == "Найновіші (Додані)":
        keywords.sort(key=lambda x: x['created_at'], reverse=True)
    elif sort_option == "Найстаріші (Додані)":
        keywords.sort(key=lambda x: x['created_at'], reverse=False)
    elif sort_option == "Нещодавно проскановані":
        keywords.sort(key=lambda x: x['last_scan_date'], reverse=True)
    elif sort_option == "Давно не скановані":
        keywords.sort(key=lambda x: x['last_scan_date'], reverse=False)

    # --- 4. МАСОВІ ДІЇ ---
    with st.container(border=True):
        c_bulk_1, c_bulk_2, c_bulk_3 = st.columns([0.5, 2, 1])
        
        # Чекбокс "ОБРАТИ ВСІ"
        with c_bulk_1:
            select_all = st.checkbox("Всі", key="select_all_kws")
        
        with c_bulk_2:
            bulk_models = st.multiselect(
                "ЛЛМ для запуску:", 
                list(MODEL_MAPPING.keys()), 
                default=["Perplexity"], 
                label_visibility="collapsed", 
                key="bulk_models_sel"
            )
        
        with c_bulk_3:
            if st.button("🚀 Запустити аналіз", use_container_width=True):
                # Збираємо ID
                selected_kws_text = []
                
                # Якщо натиснуто "Всі", беремо всі, інакше перевіряємо поштучно
                if select_all:
                    selected_kws_text = [k['keyword_text'] for k in keywords]
                else:
                    for k in keywords:
                        if st.session_state.get(f"chk_{k['id']}", False):
                            selected_kws_text.append(k['keyword_text'])
                
                if selected_kws_text:
                    with st.spinner(f"Відправляємо {len(selected_kws_text)} запитів..."):
                        n8n_trigger_analysis(proj["id"], selected_kws_text, proj.get("brand_name"), models=bulk_models)
                        st.success("Успішно! Оновіть сторінку за хвилину.")
                        # Скидаємо виділення (опціонально)
                        if select_all: st.session_state["select_all_kws"] = False
                        time.sleep(2)
                        st.rerun()
                else:
                    st.warning("Оберіть хоча б один запит.")

    # Заголовки таблиці
    h1, h2, h3, h4 = st.columns([0.5, 3, 1.5, 1])
    h2.markdown("**Запит**")
    h3.markdown("**Останній аналіз**")
    h4.markdown("**Дії**")

    # --- 5. ВИВІД СПИСКУ ---
    for k in keywords:
        with st.container(border=True):
            c1, c2, c3, c4 = st.columns([0.5, 3, 1.5, 1])
            
            # Чекбокс (якщо Select All увімкнено, то галочка стоїть автоматично)
            with c1:
                is_checked = select_all
                st.checkbox("", key=f"chk_{k['id']}", value=is_checked)
            
            # Текст
            with c2:
                st.markdown(f"**{k['keyword_text']}**")
            
            # Дата
            with c3:
                date_iso = k.get('last_scan_date')
                if date_iso and date_iso != "1970-01-01T00:00:00+00:00":
                    dt_obj = datetime.fromisoformat(date_iso.replace('Z', '+00:00'))
                    formatted_date = dt_obj.strftime("%d.%m.%Y %H:%M")
                    st.caption(f"🕒 {formatted_date}")
                else:
                    st.caption("—")
            
            # Кнопки
            with c4:
                b1, b2 = st.columns(2)
                if b1.button("🔍", key=f"det_{k['id']}", help="Детальний аналіз"):
                    st.session_state["focus_keyword_id"] = k["id"]
                    st.rerun()
                if b2.button("🗑", key=f"del_{k['id']}", help="Видалити"):
                    supabase.table("keywords").delete().eq("id", k["id"]).execute()
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
    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект.")
        return

    st.title("📡 Джерела та Репутація")
    
    tab1, tab2 = st.tabs(["🛡️ Мої Активи (Whitelist)", "🌐 Аналіз Ринку"])

    # --- TAB 1: МОЇ ОФІЦІЙНІ ДЖЕРЕЛА ---
    with tab1:
        st.markdown("Додайте сюди ваші офіційні сайти та соцмережі.")
        
        # Форма додавання
        with st.expander("➕ Додати нове джерело", expanded=False):
            with st.form("add_asset_form"):
                c1, c2 = st.columns([2, 1])
                with c1:
                    new_asset = st.text_input("URL або Домен")
                with c2:
                    asset_type = st.selectbox("Тип", ["website", "social", "article", "other"])
                
                if st.form_submit_button("Зберегти"):
                    if new_asset:
                        try:
                            supabase.table("official_assets").insert({
                                "project_id": proj["id"],
                                "domain_or_url": new_asset,
                                "type": asset_type
                            }).execute()
                            st.success(f"Джерело додано.")
                            time.sleep(0.5)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Помилка: {e}")

        st.divider()

        # --- ТАБЛИЦЯ З РЕДАГУВАННЯМ ---
        try:
            assets = supabase.table("official_assets").select("*").eq("project_id", proj["id"]).order("created_at", desc=True).execute().data
        except:
            assets = []

        if assets:
            for asset in assets:
                # Використовуємо контейнер для кожного рядка
                with st.container(border=True):
                    # Якщо натиснуто "Редагувати", показуємо форму
                    if st.session_state.get(f"edit_mode_{asset['id']}", False):
                        c_edit, c_btn = st.columns([4, 1])
                        with c_edit:
                            new_val = st.text_input("URL", value=asset['domain_or_url'], key=f"in_{asset['id']}")
                            new_type = st.selectbox("Тип", ["website", "social", "article", "other"], index=["website", "social", "article", "other"].index(asset['type']), key=f"sel_{asset['id']}")
                        
                        col_save, col_cancel = st.columns(2)
                        if col_save.button("💾 Зберегти", key=f"save_{asset['id']}"):
                            supabase.table("official_assets").update({
                                "domain_or_url": new_val,
                                "type": new_type
                            }).eq("id", asset['id']).execute()
                            st.session_state[f"edit_mode_{asset['id']}"] = False
                            st.rerun()
                            
                        if col_cancel.button("Скасувати", key=f"cancel_{asset['id']}"):
                            st.session_state[f"edit_mode_{asset['id']}"] = False
                            st.rerun()
                            
                    else:
                        # Режим перегляду
                        c1, c2, c3, c4 = st.columns([3, 1, 0.5, 0.5])
                        with c1:
                            st.markdown(f"**{asset['domain_or_url']}**")
                        with c2:
                            st.caption(asset['type'].upper())
                        with c3:
                            if st.button("✏️", key=f"edit_btn_{asset['id']}"):
                                st.session_state[f"edit_mode_{asset['id']}"] = True
                                st.rerun()
                        with c4:
                            if st.button("🗑", key=f"del_as_{asset['id']}"):
                                supabase.table("official_assets").delete().eq("id", asset['id']).execute()
                                st.rerun()
        else:
            st.info("Список порожній.")

    # --- TAB 2: АНАЛІЗ РИНКУ ---
    with tab2:
        # (Код для аналізу ринку залишається без змін з попереднього разу)
        # Просто додайте try/except блок, як було раніше
        pass 
        # ... (код з попередньої відповіді)
def show_competitors_page():
    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект.")
        return

    st.title("⚔️ Аналіз Конкурентів")
    st.caption("Кого ШІ рекомендує поруч із вами? Порівняння видимості та репутації.")

    # 1. Завантаження даних з SQL View
    try:
        data = (
            supabase.table("competitor_stats")
            .select("*")
            .eq("project_id", proj["id"])
            .execute()
            .data
        )
    except Exception as e:
        st.error(f"Помилка завантаження даних: {e}")
        data = []

    if not data:
        st.info("Даних ще недостатньо. Запустіть кілька сканувань у 'Перелік запитів', щоб ШІ знайшов конкурентів.")
        return

    # Перетворюємо в DataFrame для зручної роботи
    df = pd.DataFrame(data)

    # 2. Метрики лідера (Хто головний конкурент?)
    # Виключаємо наш бренд, щоб знайти реального ворога
    competitors_only = df[df['is_my_brand'] == False]
    
    if not competitors_only.empty:
        # Сортуємо за кількістю згадок
        top_rival = competitors_only.sort_values(by="total_mentions", ascending=False).iloc[0]
        
        # Знаходимо нас
        my_brand = df[df['is_my_brand'] == True]
        my_mentions = my_brand.iloc[0]['total_mentions'] if not my_brand.empty else 0
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Головний конкурент", top_rival['brand_name'])
        c2.metric("Його згадок", top_rival['total_mentions'], delta=int(top_rival['total_mentions'] - my_mentions), delta_color="inverse")
        c3.metric("Його тональність", f"{int(top_rival['avg_sentiment'])}/100")
    
    st.divider()

    # 3. Графік 1: КАРТА РЕПУТАЦІЇ (Scatter Plot)
    # Це найкрутіший графік для GEO.
    st.subheader("🗺️ Карта Репутації (Magic Quadrant)")
    st.caption("Чим вище — тим краще відгукуються. Чим правіше — тим частіше згадують.")

    if not df.empty:
        # Додаємо колонку кольору: Мій бренд = Фіолетовий, Інші = Сірий
        df['Color'] = df['is_my_brand'].apply(lambda x: 'Мій Бренд' if x else 'Конкурент')
        df['Size'] = df['total_mentions'] * 2 # Розмір бульбашки

        fig = px.scatter(
            df,
            x="total_mentions",
            y="avg_sentiment",
            size="Size",
            color="Color",
            text="brand_name",
            color_discrete_map={'Мій Бренд': '#8041F6', 'Конкурент': '#9EA0A5'},
            hover_data=["avg_rank"],
            height=500
        )
        # Налаштування вигляду
        fig.update_traces(textposition='top center')
        fig.update_layout(
            xaxis_title="Кількість згадок (Видимість)",
            yaxis_title="Середня тональність (Якість)",
            yaxis_range=[0, 105], # Щоб графік завжди був від 0 до 100
            showlegend=True
        )
        # Малюємо лінії середини
        fig.add_hline(y=50, line_dash="dot", line_color="lightgray")
        
        st.plotly_chart(fig, use_container_width=True)

    # 4. Графік 2: Рейтинг за часткою голосу (Bar Chart)
    st.subheader("📊 Рейтинг за часткою голосу (Share of Voice)")
    
    if not df.empty:
        # Сортуємо для краси
        df_sorted = df.sort_values(by="total_mentions", ascending=True) # Ascending для горизонтального бару
        
        fig_bar = px.bar(
            df_sorted,
            x="total_mentions",
            y="brand_name",
            orientation='h',
            text="total_mentions",
            color="is_my_brand",
            color_discrete_map={True: '#8041F6', False: '#D1D1D6'}
        )
        fig_bar.update_layout(showlegend=False, xaxis_title="Кількість згадок", yaxis_title="")
        st.plotly_chart(fig_bar, use_container_width=True)

    # 5. Детальна таблиця
    with st.expander("📋 Дивитися детальні дані таблицею"):
        # Готуємо красиву таблицю
        display_df = df[['brand_name', 'total_mentions', 'avg_sentiment', 'avg_rank', 'is_my_brand']].copy()
        display_df.columns = ['Бренд', 'Згадок', 'Тональність', 'Сер. Позиція', 'Це ми?']
        
        # Форматуємо числа
        display_df['Тональність'] = display_df['Тональність'].astype(int)
        display_df['Сер. Позиція'] = display_df['Сер. Позиція'].apply(lambda x: f"#{x:.1f}" if x else "-")
        display_df['Це ми?'] = display_df['Це ми?'].apply(lambda x: "✅" if x else "")
        
        # Сортуємо
        display_df = display_df.sort_values(by="Згадок", ascending=False)
        
        st.dataframe(
            display_df, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "Згадок": st.column_config.ProgressColumn(
                    "Частка",
                    format="%d",
                    min_value=0,
                    max_value=int(df['total_mentions'].max())
                )
            }
        )

def show_chat_page():
    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект.")
        return

    st.title(f"🤖 Virshi AI: Асистент для {proj.get('brand_name')}")
    st.caption("Задайте питання про ваші позиції, конкурентів або попросіть пораду.")

    # 1. Завантажуємо історію повідомлень
    try:
        messages = (
            supabase.table("chat_messages")
            .select("*")
            .eq("project_id", proj["id"])
            .order("created_at", desc=False) # Старі зверху
            .execute()
            .data
        )
    except:
        messages = []

    # 2. Відображаємо історію
    if not messages:
        # Привітання, якщо чат пустий
        with st.chat_message("assistant"):
            st.write(f"Привіт! Я проаналізував дані по **{proj.get('brand_name')}**. Що вас цікавить?")
            st.write("Наприклад: _'Хто мій головний конкурент?'_ або _'Напиши пост для LinkedIn про наш рейтинг'_.")

    for msg in messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    # 3. Поле вводу
    if prompt := st.chat_input("Напишіть ваше питання..."):
        # А. Показуємо питання користувача одразу
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Б. Зберігаємо питання в базу
        try:
            supabase.table("chat_messages").insert({
                "project_id": proj["id"],
                "user_id": st.session_state["user"].id,
                "role": "user",
                "content": prompt
            }).execute()
        except Exception as e:
            st.error(f"Помилка збереження: {e}")

        # В. Генеруємо відповідь (Тут буде підключення до n8n)
        with st.chat_message("assistant"):
            with st.spinner("Аналізую дані..."):
                # --- ТУТ МАЄ БУТИ ВИКЛИК N8N ---
                # response = n8n_chat_webhook(prompt, proj_id)
                
                # ПОКИ ЩО: Симуляція розумної відповіді
                time.sleep(1.5) 
                
                # Проста логіка заглушки для демо
                if "конкурент" in prompt.lower():
                    response_text = f"Вашим головним конкурентом виглядає **PrivatBank** (за кількістю згадок). Вам варто звернути увагу на їх активність у статтях на Minfin."
                elif "пост" in prompt.lower():
                    response_text = f"Ось чернетка посут:\n\n🚀 **{proj.get('brand_name')} вривається в топи!**\n\nШІ відзначають нас як лідера... (тут текст)"
                else:
                    response_text = f"Це цікаве питання про **{proj.get('brand_name')}**. Для точної відповіді мені треба зібрати більше даних сканування. Спробуйте запустити новий скан у вкладці 'Перелік запитів'."
                
                st.markdown(response_text)

        # Г. Зберігаємо відповідь асистента в базу
        try:
            supabase.table("chat_messages").insert({
                "project_id": proj["id"],
                "user_id": st.session_state["user"].id,
                "role": "assistant",
                "content": response_text
            }).execute()
        except:
            pass


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
            show_sources_page()
        elif page == "Конкуренти":
            show_competitors_page()
        elif page == "Рекомендації":
            show_recommendations_page()
        elif page == "GPT-Visibility":
            show_chat_page()
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
