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
    """
    Відправляє запит на n8n для аналізу.
    Включає: 
    1. Перевірку статусу (Gatekeeper).
    2. Мапінг назв моделей.
    3. Отримання офіційних джерел (Whitelist).
    """
    
    # 1. Мапінг назв (UI -> Technical)
    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }

    # 2. 🔒 ПЕРЕВІРКА СТАТУСУ (БЛОКУВАННЯ)
    # Отримуємо поточний проект із сесії
    current_proj = st.session_state.get("current_project", {})
    status = current_proj.get("status", "trial")
    
    # Якщо статус заблокований або термін дії вийшов - зупиняємо
    if status in ["blocked", "expired"]:
        st.error(f"⛔ Дія недоступна. Ваш статус: {status.upper()}. Будь ласка, зв'яжіться з адміністратором.")
        return False

    try:
        user_email = st.session_state["user"].email if st.session_state.get("user") else None
        
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

        # 4. ЦИКЛ ВІДПРАВКИ (По кожній моделі окремо)
        for ui_model_name in models:
            # Конвертуємо красиву назву в технічний ID
            tech_model_id = MODEL_MAPPING.get(ui_model_name, ui_model_name)

            payload = {
                "project_id": project_id,
                "keywords": keywords, 
                "brand_name": brand_name,
                "user_email": user_email,
                "provider": tech_model_id,     # Для Switch в n8n
                "models": [tech_model_id],     # Для сумісності
                "official_assets": official_assets # Передаємо Whitelist
            }
            
            try:
                # Відправка на вебхук
                response = requests.post(N8N_ANALYZE_URL, json=payload, timeout=10)
                
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
    Майстер створення першого проекту для нового користувача.
    """
    st.markdown("## 🚀 Налаштування вашого Проекту")
    st.info("Вітаємо! Створіть свій перший проект, щоб почати аналіз.")

    with st.form("onboarding_form"):
        # Крок 1: Основні дані
        st.subheader("1. Дані про бренд")
        c1, c2 = st.columns(2)
        with c1:
            brand_name = st.text_input("Назва бренду", placeholder="Наприклад: Monobank")
        with c2:
            domain = st.text_input("Офіційний сайт (Домен)", placeholder="monobank.ua")
        
        region = st.selectbox("Регіон", ["UA", "US", "EU", "Global"], index=0)
        
        # Крок 2: Офіційні ресурси
        st.subheader("2. Офіційні джерела (Whitelist)")
        st.caption("Вкажіть ваші соцмережі та сайти через кому. Ми будемо позначати їх як 'Офіційні'.")
        assets_text = st.text_area("Список URL", placeholder="https://instagram.com/mono, https://t.me/monobankua", help="Розділяйте комою або новим рядком")

        # Крок 3: Перші запити (Опціонально)
        st.subheader("3. Перші запити для моніторингу")
        keywords_text = st.text_area("Введіть 3-5 запитів (по одному в рядок)", placeholder="курси валют монобанк\nяк відкрити карту моно", height=100)

        submitted = st.form_submit_button("🚀 Створити Проект", type="primary")

        if submitted:
            if not brand_name or not domain:
                st.error("Будь ласка, вкажіть Назву бренду та Домен.")
            else:
                try:
                    user = st.session_state["user"]
                    
                    # 1. Створюємо проект
                    proj_res = supabase.table("projects").insert({
                        "user_id": user.id,
                        "brand_name": brand_name,
                        "domain": domain,
                        "region": region,
                        "status": "trial" # По дефолту тріал
                    }).execute()
                    
                    if proj_res.data:
                        new_proj = proj_res.data[0]
                        proj_id = new_proj["id"]
                        
                        # 2. Додаємо асети (Whitelist)
                        assets_list = [a.strip() for a in assets_text.replace("\n", ",").split(",") if a.strip()]
                        assets_list.append(domain) # Додаємо сам домен теж
                        
                        if assets_list:
                            assets_data = [{"project_id": proj_id, "domain_or_url": a, "type": "website"} for a in assets_list]
                            supabase.table("official_assets").insert(assets_data).execute()

                        # 3. Додаємо ключові слова
                        kws_list = [k.strip() for k in keywords_text.split("\n") if k.strip()]
                        if kws_list:
                            kws_data = [{"project_id": proj_id, "keyword_text": k, "is_active": True} for k in kws_list]
                            supabase.table("keywords").insert(kws_data).execute()

                        # 4. Фінал
                        st.success("Проект успішно створено!")
                        st.session_state["current_project"] = new_proj
                        time.sleep(1)
                        st.rerun()
                        
                except Exception as e:
                    st.error(f"Помилка при створенні: {e}")


# =========================
# 6. DASHBOARD
# =========================

def show_competitors_page():
    """
    Сторінка глибокого конкурентного аналізу.
    Версія: Вкладки + Спрощені графіки (Bar Charts).
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
    with st.expander("⚙️ Фільтри аналізу", expanded=False):
        c1, c2 = st.columns(2)
        with c1:
            all_models = list(MODEL_MAPPING.keys())
            sel_models = st.multiselect("Фільтр по ЛЛМ:", all_models, default=all_models)
            sel_tech_models = [MODEL_MAPPING[m] for m in sel_models]

        with c2:
            all_kws = df_full['keyword_text'].dropna().unique().tolist()
            sel_kws = st.multiselect("Фільтр по Запитах:", all_kws, default=all_kws)

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
    def sentiment_to_score(s):
        if s == 'Позитивний': return 100
        if s == 'Негативний': return 0
        return 50 # Нейтральний
    
    df_filtered['sent_score_num'] = df_filtered['sentiment_score'].apply(sentiment_to_score)

    stats = df_filtered.groupby('brand_name').agg(
        Mentions=('id_x', 'count'),
        Avg_Rank=('rank_position', 'mean'),
        Avg_Sentiment=('sent_score_num', 'mean'),
        Is_My_Brand=('is_my_brand', 'max')
    ).reset_index()

    # --- 4. ВІДОБРАЖЕННЯ (ВКЛАДКИ) ---
    st.write("") # Spacer
    tab_list, tab_sov, tab_rep = st.tabs(["📋 Детальний рейтинг", "📊 Share of Voice", "⭐ Репутація"])

    # === TAB 1: ДЕТАЛЬНИЙ РЕЙТИНГ (ТАБЛИЦЯ) ===
    with tab_list:
        st.markdown("##### 📋 Зведена таблиця показників")
        
        # Підготовка таблиці
        display_df = stats.copy()
        display_df = display_df.sort_values('Mentions', ascending=False)
        
        # Красиве відображення
        display_df_show = display_df[['brand_name', 'Mentions', 'Avg_Sentiment', 'Avg_Rank', 'Is_My_Brand']].copy()
        display_df_show.columns = ['Бренд', 'Згадок', 'Репутація', 'Сер. Позиція', 'Це ми?']
        
        display_df_show['Сер. Позиція'] = display_df_show['Сер. Позиція'].apply(lambda x: f"#{x:.1f}")
        display_df_show['Це ми?'] = display_df_show['Це ми?'].apply(lambda x: True if x else False)

        st.dataframe(
            display_df_show,
            use_container_width=True,
            column_config={
                "Згадок": st.column_config.ProgressColumn("Згадок", format="%d", min_value=0, max_value=int(stats['Mentions'].max())),
                "Репутація": st.column_config.NumberColumn("Репутація", format="%d / 100"),
                "Це ми?": st.column_config.CheckboxColumn("Наш бренд?", disabled=True)
            },
            hide_index=True
        )

    # === TAB 2: SHARE OF VOICE (КІЛЬКІСТЬ ЗГАДОК) ===
    with tab_sov:
        st.markdown("##### 📊 Хто найгучніший? (Кількість згадок)")
        st.caption("Показує частку ринку у відповідях ШІ. Чим довша смужка, тим частіше бренд рекомендують.")
        
        # Сортування: Лідер зверху
        sov_data = stats.sort_values('Mentions', ascending=True) # Ascending для горизонтального бару (лідер буде зверху)
        
        fig_sov = px.bar(
            sov_data,
            x="Mentions",
            y="brand_name",
            orientation='h',
            text="Mentions",
            color="Is_My_Brand",
            color_discrete_map={True: '#00C896', False: '#E0E0E0'}, # Зелений для нас
            height=500
        )
        fig_sov.update_layout(
            yaxis_title="",
            xaxis_title="Кількість згадок",
            showlegend=False,
            plot_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=0, r=0, t=30, b=0)
        )
        st.plotly_chart(fig_sov, use_container_width=True)

    # === TAB 3: РЕПУТАЦІЯ (ТОНАЛЬНІСТЬ) - НОВИЙ ГРАФІК ===
    with tab_rep:
        st.markdown("##### ⭐ Хто найякісніший? (Середня тональність)")
        st.caption("Рейтинг брендів за якістю відгуків ШІ (0 - Негатив, 100 - Позитив).")
        
        # Сортування: Найкращі зверху
        rep_data = stats.sort_values('Avg_Sentiment', ascending=True)
        
        # Стовпчикова діаграма
        fig_rep = px.bar(
            rep_data,
            x="Avg_Sentiment",
            y="brand_name",
            orientation='h',
            text=rep_data['Avg_Sentiment'].apply(lambda x: f"{x:.0f}"), # Форматуємо текст на барі
            color="Is_My_Brand",
            color_discrete_map={True: '#00C896', False: '#D1D1D6'}, # Зелений для нас, сірий для інших
            height=500
        )
        
        fig_rep.update_layout(
            xaxis=dict(range=[0, 105], title="Бали репутації (0-100)"),
            yaxis_title="",
            showlegend=False,
            plot_bgcolor='rgba(0,0,0,0)',
            margin=dict(l=0, r=0, t=30, b=0)
        )
        
        # Додаємо лінію "Нейтральності"
        fig_rep.add_vline(x=50, line_width=1, line_dash="dash", line_color="gray", annotation_text="Нейтрально")
        
        st.plotly_chart(fig_rep, use_container_width=True)

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
        
        if not scan_ids:
            st.warning(f"🔍 За період з {start_date.strftime('%d.%m.%Y')} по {end_date.strftime('%d.%m.%Y')} даних не знайдено.")
            return 

        # B. Згадки та Джерела
        mentions_resp = supabase.table("brand_mentions").select("*").in_("scan_result_id", scan_ids).execute()
        sources_resp = supabase.table("extracted_sources").select("*").in_("scan_result_id", scan_ids).execute()
        keywords_resp = supabase.table("keywords").select("id, keyword_text").eq("project_id", proj["id"]).execute()
        
        df_mentions = pd.DataFrame(mentions_resp.data)
        df_sources = pd.DataFrame(sources_resp.data)
        kw_map = {k['id']: k['keyword_text'] for k in keywords_resp.data}

    except Exception as e:
        st.error(f"Помилка завантаження: {e}")
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
    Сторінка списку запитів з можливістю редагування CRON-статусу.
    """
    import pandas as pd
    import streamlit as st
    from datetime import datetime
    import time

    MODEL_MAPPING = {
        "Perplexity": "perplexity",
        "OpenAI GPT": "gpt-4o",
        "Google Gemini": "gemini-1.5-pro"
    }

    if "kw_input_count" not in st.session_state:
        st.session_state["kw_input_count"] = 1

    proj = st.session_state.get("current_project")
    if not proj:
        st.info("Спочатку створіть проект в онбордингу.")
        return

    if st.session_state.get("focus_keyword_id"):
        show_keyword_details(st.session_state["focus_keyword_id"])
        return

    st.title("📋 Перелік запитів")

    # ========================================================
    # 1. БЛОК ДОДАВАННЯ (Динамічний)
    # ========================================================
    with st.expander("➕ Додати нові запити", expanded=False):
        with st.container(border=True):
            st.markdown("##### 📝 Введіть запити")
            
            for i in range(st.session_state["kw_input_count"]):
                st.text_input(f"Запит #{i+1}", key=f"new_kw_input_{i}", placeholder="Наприклад: Купити квитки Київ Варшава")

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
                selected_models = st.multiselect("Оберіть ЛЛМ:", list(MODEL_MAPPING.keys()), default=["Perplexity"])
            
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
                            # Вставка в БД (за замовчуванням cron = false)
                            insert_data = [{"project_id": proj["id"], "keyword_text": kw, "is_active": True, "is_cron_active": False} for kw in new_keywords_list]
                            res = supabase.table("keywords").insert(insert_data).execute()
                            
                            if res.data:
                                # Запуск першого сканування
                                with st.spinner(f"Запускаємо аналіз..."):
                                    n8n_trigger_analysis(proj["id"], new_keywords_list, proj.get("brand_name"), models=selected_models)
                                
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
    # 2. ТАБЛИЦЯ ЗАПИТІВ (DATA EDITOR)
    # ========================================================
    try:
        # А. Отримуємо запити
        # Важливо: сортуємо по ID, щоб рядки не стрибали при редагуванні
        keywords = supabase.table("keywords").select("*").eq("project_id", proj["id"]).order("id", desc=True).execute().data
        
        # Б. Отримуємо дати останнього скану
        last_scans_resp = supabase.table("scan_results")\
            .select("keyword_id, created_at")\
            .eq("project_id", proj["id"])\
            .order("created_at", desc=True)\
            .execute()
            
        last_scan_map = {}
        if last_scans_resp.data:
            for s in last_scans_resp.data:
                if s['keyword_id'] not in last_scan_map:
                    # Форматуємо дату: 2023-12-01 14:00
                    dt = datetime.fromisoformat(s['created_at'].replace('Z', '+00:00'))
                    last_scan_map[s['keyword_id']] = dt.strftime("%d.%m %H:%M")
        
        # Підготовка даних для Editor
        for k in keywords:
            k['last_scan'] = last_scan_map.get(k['id'], "-")
            k['delete'] = False # Колонка для видалення

        if not keywords:
            st.info("Список порожній.")
            return

        # Створюємо DataFrame
        df = pd.DataFrame(keywords)
        
        # Обираємо потрібні колонки і порядок
        # keyword_text, is_cron_active, last_scan, delete
        df_editor = df[['id', 'keyword_text', 'is_cron_active', 'last_scan', 'delete']].copy()

        # --- ВІДОБРАЖЕННЯ DATA EDITOR ---
        st.markdown("### 📋 Управління запитами")
        st.caption("Вмикайте 'Авто-Скан' для запитів, які хочете моніторити щодня.")

        edited_df = st.data_editor(
            df_editor,
            column_config={
                "id": None, # Ховаємо ID
                "keyword_text": st.column_config.TextColumn("Текст запиту", disabled=True, width="large"),
                "is_cron_active": st.column_config.CheckboxColumn("⏰ Авто-Скан", help="Вмикає щоденний моніторинг", default=False),
                "last_scan": st.column_config.TextColumn("Останній аналіз", disabled=True),
                "delete": st.column_config.CheckboxColumn("🗑️ Видалити", default=False)
            },
            hide_index=True,
            use_container_width=True,
            key="keywords_editor"
        )

        # ========================================================
        # 3. ОБРОБКА ЗМІН (ЗБЕРЕЖЕННЯ)
        # ========================================================
        
        # Перевіряємо, чи були зміни в едіторі
        # Порівнюємо старий df і новий edited_df
        
        # 1. Обробка зміни CRON
        # Знаходимо рядки, де змінився статус is_cron_active
        # Оскільки data_editor повертає повний df, ми можемо просто пройтися по ньому, 
        # але це буде багато запитів. Краще реагувати на кнопку "Зберегти зміни" або використовувати session state.
        # Але Streamlit data_editor зберігає стан автоматично.
        
        # Найпростіший спосіб: знайти різницю
        changes_detected = False
        
        # Проходимо по рядках і шукаємо зміни або видалення
        to_delete_ids = []
        to_update_cron = []

        for index, row in edited_df.iterrows():
            original_row = df[df['id'] == row['id']].iloc[0]
            
            # Перевірка на видалення
            if row['delete']:
                to_delete_ids.append(row['id'])
                changes_detected = True
                continue # Якщо видаляємо, то крон не важливий
            
            # Перевірка на зміну крона
            # Порівнюємо булеві значення (True/False)
            if bool(row['is_cron_active']) != bool(original_row['is_cron_active']):
                to_update_cron.append({"id": row['id'], "is_cron_active": row['is_cron_active']})
                changes_detected = True

        if changes_detected:
            if st.button("💾 Застосувати зміни", type="primary"):
                try:
                    # 1. Видалення
                    if to_delete_ids:
                        supabase.table("keywords").delete().in_("id", to_delete_ids).execute()
                    
                    # 2. Оновлення Крона
                    for item in to_update_cron:
                        supabase.table("keywords").update({"is_cron_active": item["is_cron_active"]}).eq("id", item["id"]).execute()
                    
                    st.success("Зміни збережено!")
                    time.sleep(1)
                    st.rerun()
                except Exception as e:
                    st.error(f"Помилка збереження: {e}")

    except Exception as e:
        st.error(f"Помилка інтерфейсу: {e}")

    # --- МАСОВІ ДІЇ (Окремо, бо data_editor не підтримує вибір рядків для дій, тільки редагування) ---
    st.divider()
    with st.expander("🛠️ Ручний запуск аналізу"):
        c_bulk_1, c_bulk_2 = st.columns([2, 1])
        with c_bulk_1:
            bulk_models = st.multiselect("Оберіть моделі:", list(MODEL_MAPPING.keys()), default=["Perplexity"])
        with c_bulk_2:
            st.write("")
            st.write("")
            # Тут ми беремо всі активні запити
            if st.button("🚀 Просканувати ВСІ запити", use_container_width=True):
                all_kws_text = [k['keyword_text'] for k in keywords]
                if all_kws_text:
                    with st.spinner(f"Запускаємо {len(all_kws_text)} запитів..."):
                        n8n_trigger_analysis(proj["id"], all_kws_text, proj.get("brand_name"), models=bulk_models)
                        st.success("Запущено!")

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
    Сторінка управління джерелами та аналізу репутації.
    Версія: 3 вкладки, окремі фільтри, безпечна обробка колонок.
    """
    import pandas as pd
    import streamlit as st
    
    # 0. ПІДКЛЮЧЕННЯ
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
    ALL_MODELS_KEYS = list(MODEL_MAPPING.keys())

    st.title("📡 Джерела та Репутація")
    
    # === 1. ЗАВАНТАЖЕННЯ ДАНИХ ===
    try:
        # Whitelist
        assets_resp = supabase.table("official_assets").select("*").eq("project_id", proj["id"]).order("created_at", desc=True).execute()
        assets = assets_resp.data if assets_resp.data else []
        whitelist = [a['domain_or_url'] for a in assets]

        # Скани та Джерела
        scans_q = supabase.table("scan_results").select("id, provider").eq("project_id", proj["id"]).execute()
        scan_map = {s['id']: s['provider'] for s in scans_q.data}
        scan_ids = list(scan_map.keys())

        if scan_ids:
            sources_resp = supabase.table("extracted_sources").select("*").in_("scan_result_id", scan_ids).execute()
            df_sources = pd.DataFrame(sources_resp.data)
        else:
            df_sources = pd.DataFrame()

    except Exception as e:
        st.error(f"Помилка завантаження даних: {e}")
        return

    # === 2. ПОПЕРЕДНЯ ОБРОБКА ===
    if not df_sources.empty:
        # Додаємо провайдера до кожного джерела
        df_sources['provider'] = df_sources['scan_result_id'].map(scan_map)
        
        # Гарантуємо наявність колонок, щоб не було помилок
        if 'domain' not in df_sources.columns: df_sources['domain'] = None
        if 'url' not in df_sources.columns: df_sources['url'] = None
    
    # === 3. ВКЛАДКИ ===
    tab1, tab2, tab3 = st.tabs(["🛡️ Мої Активи", "🌐 Ренкінг доменів", "📄 Топ Сторінок (URL)"])

    # -------------------------------------------------------
    # TAB 1: МОЇ АКТИВИ (Без фільтрів)
    # -------------------------------------------------------
    with tab1:
        st.markdown("##### 🟢 Ваші офіційні ресурси")
        st.caption("Домени, які система позначатиме як 'Офіційні' (✅).")
        
        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            new_asset = st.text_input("URL/Домен", placeholder="example.com")
        with c2:
            asset_type = st.selectbox("Тип", ["website", "social", "article"], label_visibility="visible")
        with c3:
            st.write("") 
            st.write("") 
            if st.button("➕ Додати", use_container_width=True):
                if new_asset:
                    try:
                        clean = new_asset.replace("https://", "").replace("http://", "").strip().rstrip("/")
                        supabase.table("official_assets").insert({
                            "project_id": proj["id"], "domain_or_url": clean, "type": asset_type
                        }).execute()
                        st.success("Додано!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Помилка: {e}")

        if assets:
            st.markdown("---")
            for asset in assets:
                with st.container(border=True):
                    c_txt, c_type, c_del = st.columns([4, 1, 0.5])
                    c_txt.markdown(f"**{asset['domain_or_url']}**")
                    c_type.caption(asset['type'].upper())
                    if c_del.button("🗑", key=f"del_{asset['id']}"):
                        supabase.table("official_assets").delete().eq("id", asset['id']).execute()
                        st.rerun()
        else:
            st.info("Список пустий. Додайте ваш сайт.")

    # -------------------------------------------------------
    # TAB 2: РЕНКІНГ ДОМЕНІВ (Фільтр + Таблиця)
    # -------------------------------------------------------
    with tab2:
        # 1. Фільтр
        c_filter, _ = st.columns([2, 2])
        with c_filter:
            sel_models_tab2 = st.multiselect(
                "Фільтр ЛЛМ:", 
                ALL_MODELS_KEYS, 
                default=ALL_MODELS_KEYS, 
                key="filter_domains"
            )
        
        # 2. Фільтрація
        if not df_sources.empty and sel_models_tab2:
            sel_tech = [MODEL_MAPPING[m] for m in sel_models_tab2]
            mask = df_sources['provider'].apply(lambda x: any(t in str(x) for t in sel_tech))
            df_tab2 = df_sources[mask]
        else:
            df_tab2 = pd.DataFrame()

        st.markdown(f"##### 🏆 Топ Доменів")
        
        # 3. Перевірка та відображення
        if not df_tab2.empty and df_tab2['domain'].notna().any():
            # Групуємо по домену
            domain_stats = df_tab2.groupby('domain').agg(
                Mentions=('id', 'count'),
                Queries=('scan_result_id', 'nunique')
            ).reset_index().sort_values('Mentions', ascending=False)

            def check_off(d): return any(w in str(d) for w in whitelist)
            domain_stats['Type'] = domain_stats['domain'].apply(lambda x: "✅ Офіційний" if check_off(x) else "🔗 Зовнішній")
            
            show_dom = domain_stats[['domain', 'Type', 'Mentions', 'Queries']].copy()
            show_dom.columns = ['Домен', 'Тип', 'К-сть цитувань', 'Охоплення запитів']

            st.dataframe(
                show_dom, 
                use_container_width=True,
                column_config={
                    "К-сть цитувань": st.column_config.ProgressColumn("Цитувань", format="%d", min_value=0, max_value=int(show_dom['К-сть цитувань'].max()))
                },
                hide_index=True
            )
        else:
            st.info("Доменів не знайдено (перевірте, чи заповнюється колонка 'domain' в базі).")

    # -------------------------------------------------------
    # TAB 3: ТОП СТОРІНОК (Фільтр + Таблиця)
    # -------------------------------------------------------
    with tab3:
        # 1. Фільтр
        c_filter_url, _ = st.columns([2, 2])
        with c_filter_url:
            sel_models_tab3 = st.multiselect(
                "Фільтр ЛЛМ:", 
                ALL_MODELS_KEYS, 
                default=ALL_MODELS_KEYS, 
                key="filter_urls"
            )

        # 2. Фільтрація
        if not df_sources.empty and sel_models_tab3:
            sel_tech_url = [MODEL_MAPPING[m] for m in sel_models_tab3]
            mask_url = df_sources['provider'].apply(lambda x: any(t in str(x) for t in sel_tech_url))
            df_tab3 = df_sources[mask_url]
        else:
            df_tab3 = pd.DataFrame()

        st.markdown("##### 📄 Топ Конкретних Сторінок (URL)")
        
        # 3. Перевірка та відображення
        if not df_tab3.empty and df_tab3['url'].notna().any():
            # Беремо тільки не пусті URL
            df_urls = df_tab3[df_tab3['url'].notna() & (df_tab3['url'] != "")]
            
            if not df_urls.empty:
                url_stats = df_urls.groupby('url').agg(
                    Mentions=('id', 'count')
                ).reset_index().sort_values('Mentions', ascending=False).head(100)

                st.dataframe(
                    url_stats,
                    use_container_width=True,
                    column_config={
                        "url": st.column_config.LinkColumn("Посилання"),
                        "Mentions": st.column_config.NumberColumn("К-сть цитувань")
                    },
                    hide_index=True
                )
            else:
                st.info("URL-адреси відсутні.")
        else:
            st.info("Немає даних URL. (Переконайтеся, що n8n записує поле 'url').")
            
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

def show_admin_page():
    """
    Повноцінна CRM для Адміністратора.
    Функціонал: Огляд всіх клієнтів, Створення, Редагування, Статистика.
    """
    import pandas as pd
    import streamlit as st
    import time
    
    # Перевірка доступу (на всяк випадок)
    if st.session_state.get("role") != "admin":
        st.error("⛔ Доступ заборонено.")
        return

    st.title("🛡️ Admin Panel (CRM)")
    
    # Стилізація метрик
    st.markdown("""
    <style>
        .metric-box {
            background-color: #F8F9FA;
            border: 1px solid #E0E0E0;
            border-radius: 8px;
            padding: 15px;
            text-align: center;
        }
        .metric-val { font-size: 24px; font-weight: bold; color: #00C896; }
        .metric-lbl { font-size: 14px; color: #666; }
    </style>
    """, unsafe_allow_html=True)

    # Вкладки адмінки
    tab_list, tab_create, tab_edit = st.tabs(["📋 Список Клієнтів", "➕ Створити Клієнта", "✏️ Редагування"])

    # ========================================================
    # TAB 1: СПИСОК КЛІЄНТІВ (ОГЛЯД)
    # ========================================================
    with tab_list:
        if st.button("🔄 Оновити дані"):
            st.rerun()

        try:
            # 1. Отримуємо всі проекти
            projects = supabase.table("projects").select("*").order("created_at", desc=True).execute().data
            
            if projects:
                # Підрахунок загальних метрик
                total_clients = len(projects)
                active_clients = len([p for p in projects if p.get('status') == 'active'])
                
                # Виводимо плашки зверху
                c1, c2, c3 = st.columns(3)
                c1.markdown(f"<div class='metric-box'><div class='metric-val'>{total_clients}</div><div class='metric-lbl'>Всього клієнтів</div></div>", unsafe_allow_html=True)
                c2.markdown(f"<div class='metric-box'><div class='metric-val'>{active_clients}</div><div class='metric-lbl'>Активних (Paid)</div></div>", unsafe_allow_html=True)
                c3.markdown(f"<div class='metric-box'><div class='metric-val'>{total_clients - active_clients}</div><div class='metric-lbl'>Тріал (Trial)</div></div>", unsafe_allow_html=True)
                
                st.write("") # Відступ

                # 2. Збираємо детальну статистику по кожному клієнту
                client_data = []
                
                with st.spinner("Завантаження статистики по клієнтах..."):
                    for p in projects:
                        pid = p['id']
                        
                        # А. Кількість ключових слів
                        kw_res = supabase.table("keywords").select("id", count="exact").eq("project_id", pid).execute()
                        kw_count = kw_res.count if kw_res.count is not None else 0
                        
                        # Б. Кількість запусків (Scan Runs)
                        scan_res = supabase.table("scan_results").select("id", count="exact").eq("project_id", pid).execute()
                        scan_count = scan_res.count if scan_res.count is not None else 0
                        
                        # В. Офіційні джерела (список)
                        assets_res = supabase.table("official_assets").select("domain_or_url").eq("project_id", pid).execute()
                        assets_list = [a['domain_or_url'] for a in assets_res.data]
                        assets_str = ", ".join(assets_list) if assets_list else "-"

                        # Г. CRON Статус (НОВЕ)
                        is_cron = p.get("cron_enabled", False)
                        cron_status = "✅ ON" if is_cron else "⏸️ OFF"
                        cron_freq = p.get("cron_frequency", "-") if is_cron else "-"

                        client_data.append({
                            "ID": pid,
                            "User (Email)": p.get("user_id", "N/A"),
                            "Бренд": p.get("brand_name"),
                            "Домен": p.get("domain"),
                            "Регіон": p.get("region", "UA"),
                            "Статус": p.get("status", "trial").upper(),
                            "CRON": cron_status,    # <--- Додано
                            "Частота": cron_freq,   # <--- Додано
                            "Запитів": kw_count,
                            "Аналізів": scan_count,
                            "Джерела": assets_str,
                            "Створено": p.get("created_at")[:10] if p.get("created_at") else "-"
                        })
                
                df = pd.DataFrame(client_data)
                
                # 3. Відображення таблиці
                st.dataframe(
                    df,
                    use_container_width=True,
                    column_config={
                        "ID": st.column_config.TextColumn("ID", help="Скопіюйте це ID для редагування", width="small"),
                        "Статус": st.column_config.TextColumn("Статус", help="Trial або Active", width="small"),
                        "CRON": st.column_config.TextColumn("Авто-Скан", width="small"), # <--- Додано
                        "Запитів": st.column_config.ProgressColumn("Запитів", format="%d", min_value=0, max_value=max(df["Запитів"].max(), 10)),
                        "Аналізів": st.column_config.NumberColumn("Запусків"),
                        "Джерела": st.column_config.TextColumn("Whitelist", width="medium")
                    },
                    hide_index=True
                )
            else:
                st.info("У базі поки немає проектів.")
                
        except Exception as e:
            st.error(f"Помилка завантаження адмінки: {e}")

    # ========================================================
    # TAB 2: СТВОРИТИ КЛІЄНТА (ONBOARDING FOR ADMIN)
    # ========================================================
    with tab_create:
        st.markdown("##### 👤 Додати нового клієнта")
        st.caption("Ви створюєте Проект і налаштування. Користувач зможе увійти, використовуючи Email (User ID).")
        
        with st.form("admin_create_client_form"):
            c1, c2 = st.columns(2)
            with c1:
                # Тут вводимо Email або UUID користувача з Auth
                new_uid = st.text_input("User ID / Email", help="Вкажіть email, під яким користувач буде логінитись")
                new_brand = st.text_input("Назва Бренду", placeholder="Напр. Nova Poshta")
            
            with c2:
                new_domain = st.text_input("Домен сайту", placeholder="novaposhta.ua")
                new_region = st.selectbox("Регіон", ["UA", "US", "EU", "Global"])
            
            new_status = st.selectbox("Початковий статус", ["trial", "active"])
            
            st.markdown("**Налаштування:**")
            new_assets = st.text_area("Офіційні джерела (Whitelist)", placeholder="https://instagram.com/nova...\nhttps://facebook.com/...", help="По одному в рядок або через кому")
            new_kws = st.text_area("Початкові запити (Ключові слова)", placeholder="доставка посилок\nціна доставки", help="По одному в рядок")

            submitted_create = st.form_submit_button("✅ Створити Клієнта", type="primary")
            
            if submitted_create:
                if new_uid and new_brand:
                    try:
                        # 1. Створення запису в projects
                        res = supabase.table("projects").insert({
                            "user_id": new_uid, # Прив'язка до юзера
                            "brand_name": new_brand,
                            "domain": new_domain,
                            "region": new_region,
                            "status": new_status
                        }).execute()
                        
                        if res.data:
                            new_pid = res.data[0]['id']
                            
                            # 2. Додавання джерел
                            if new_assets:
                                asset_list = [a.strip() for a in new_assets.replace("\n", ",").split(",") if a.strip()]
                                if asset_list:
                                    asset_data = [{"project_id": new_pid, "domain_or_url": a, "type": "website"} for a in asset_list]
                                    supabase.table("official_assets").insert(asset_data).execute()
                            
                            # 3. Додавання запитів
                            if new_kws:
                                kw_list = [k.strip() for k in new_kws.split("\n") if k.strip()]
                                if kw_list:
                                    kw_data = [{"project_id": new_pid, "keyword_text": k, "is_active": True} for k in kw_list]
                                    supabase.table("keywords").insert(kw_data).execute()

                            st.success(f"Клієнт {new_brand} успішно створений! ID: {new_pid}")
                            time.sleep(1)
                            st.rerun()
                    except Exception as e:
                        st.error(f"Помилка створення: {e}")
                else:
                    st.warning("Вкажіть User ID та Назву бренду.")

   # ========================================================
    # TAB 3: РЕДАГУВАННЯ (ЗМІНА СТАТУСУ, КРОН)
    # ========================================================
    with tab_edit:
        st.markdown("##### ✏️ Керування існуючим проектом")
        
        # Завантажуємо список для вибору
        try:
            all_projs = supabase.table("projects").select("id, brand_name, user_id").execute().data
            # Формат: "Brand (Email)"
            proj_options = {f"{p['brand_name']} ({p.get('user_id')})": p['id'] for p in all_projs}
            
            selected_label = st.selectbox("Оберіть клієнта для редагування:", list(proj_options.keys()), index=None)
            
            if selected_label:
                pid = proj_options[selected_label]
                
                # Завантажуємо поточні дані
                curr_data = supabase.table("projects").select("*").eq("id", pid).single().execute().data
                
                st.divider()
                
                with st.form("edit_client_form"):
                    st.subheader("1. Загальні налаштування")
                    c1, c2 = st.columns(2)
                    with c1:
                        edit_brand = st.text_input("Назва бренду", value=curr_data.get("brand_name"))
                        # Знаходимо індекс поточного статусу
                        status_opts = ["trial", "active", "expired", "blocked"]
                        curr_status = curr_data.get("status", "trial")
                        st_idx = status_opts.index(curr_status) if curr_status in status_opts else 0
                        
                        edit_status = st.selectbox("Статус (План)", status_opts, index=st_idx)
                    
                    with c2:
                        region_opts = ["UA", "US", "EU", "Global"]
                        curr_reg = curr_data.get("region", "UA")
                        reg_idx = region_opts.index(curr_reg) if curr_reg in region_opts else 0
                        
                        edit_region = st.selectbox("Регіон", region_opts, index=reg_idx)
                        
                        # Моделі поки залишаємо візуально (можна додати логіку збереження в JSON пізніше)
                        st.multiselect("Активні моделі (Доступ)", ["Perplexity", "GPT-4o", "Gemini"], default=["Perplexity", "GPT-4o", "Gemini"], disabled=True)

                    # --- БЛОК CRON (НОВИЙ) ---
                    st.divider()
                    st.subheader("2. Автоматизація (CRON)")
                    
                    cc1, cc2 = st.columns(2)
                    with cc1:
                        # Чекбокс бере значення з бази (Default: False)
                        edit_cron_enabled = st.checkbox("✅ Увімкнути авто-сканування", value=curr_data.get("cron_enabled", False))
                    
                    with cc2:
                        # Частота береться з бази (Default: daily)
                        freq_opts = ["daily", "weekly", "monthly"]
                        curr_freq = curr_data.get("cron_frequency", "daily")
                        freq_idx = freq_opts.index(curr_freq) if curr_freq in freq_opts else 0
                        
                        edit_cron_freq = st.selectbox("Частота запуску", freq_opts, index=freq_idx)

                    st.markdown("---")
                    st.caption(f"Project ID: {pid} | Created: {curr_data.get('created_at')}")

                    submitted_edit = st.form_submit_button("💾 Зберегти зміни", type="primary")
                    
                    if submitted_edit:
                        try:
                            # Оновлюємо ВСІ поля, включаючи CRON
                            supabase.table("projects").update({
                                "brand_name": edit_brand,
                                "status": edit_status,
                                "region": edit_region,
                                "cron_enabled": edit_cron_enabled,   # <--- Зберігаємо статус крона
                                "cron_frequency": edit_cron_freq     # <--- Зберігаємо частоту
                            }).eq("id", pid).execute()
                            
                            st.success("Налаштування проекту оновлено!")
                            time.sleep(1)
                            st.rerun()
                        except Exception as e:
                            st.error(f"Помилка оновлення: {e}")

        except Exception as e:
            st.error(f"Помилка завантаження списку: {e}")


def main():
    # 1. Перевірка сесії
    check_session()

    # 2. Якщо не залогінений -> Логін
    if not st.session_state.get("user"):
        login_page()
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
            st.image("logo.png", width=150) # Або текст
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
