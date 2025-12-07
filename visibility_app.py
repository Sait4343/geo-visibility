import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client, Client
from streamlit_option_menu import option_menu
from datetime import datetime, timedelta
import time

# --- 1. CONFIGURATION & SETUP ---
st.set_page_config(
    page_title="GEO-Analyst | AI Visibility Platform",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize Session State
if 'user' not in st.session_state:
    st.session_state['user'] = None
if 'selected_project' not in st.session_state:
    st.session_state['selected_project'] = None
if 'chat_history' not in st.session_state:
    st.session_state['chat_history'] = []

# Supabase Connection
try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
except:
    # Замініть на свої дані, якщо secrets не налаштовані
    SUPABASE_URL = "https://your-project.supabase.co" 
    SUPABASE_KEY = "your-anon-key"

@st.cache_resource
def init_connection():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

supabase: Client = init_connection()

# --- 2. AUTHENTICATION ---
def login():
    st.markdown("<h1 style='text-align: center;'>GEO-Analyst 🌍</h1>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center;'>AI Brand Visibility Intelligence</p>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("login_form"):
            email = st.text_input("Email")
            password = st.text_input("Password", type="password")
            submit = st.form_submit_button("Sign In", use_container_width=True)
            
            if submit:
                try:
                    res = supabase.auth.sign_in_with_password({"email": email, "password": password})
                    st.session_state['user'] = res.user
                    # Mock roles
                    user_role = 'admin' if 'admin' in email else 'user' 
                    st.session_state['role'] = user_role
                    st.session_state['balance'] = 1500
                    st.success("Login successful!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Login failed: {e}")

def logout():
    supabase.auth.sign_out()
    st.session_state['user'] = None
    st.session_state['selected_project'] = None
    st.rerun()

# --- 3. DATA FETCHING FUNCTIONS ---
def get_projects(user_id, role):
    try:
        if role == 'admin':
            response = supabase.table('projects').select("*").execute()
        else:
            response = supabase.table('projects').select("*").eq('user_id', user_id).execute()
        return pd.DataFrame(response.data)
    except Exception as e:
        return pd.DataFrame()

def get_dashboard_stats(project_id):
    try:
        response = supabase.table('dashboard_stats').select("*").eq('project_id', project_id).execute()
        if response.data:
            return response.data[0]
        return None
    except Exception:
        return None

# --- NEW: Helper to prepare data for the Detailed View ---
def get_scan_details(keyword_id, project_name):
    """
    Збирає складну структуру даних для відображення у вкладках (Tabs).
    Об'єднує scan_results, brand_mentions та extracted_sources.
    """
    results_list = []
    
    # 1. Отримуємо всі сканування для цього слова
    scans = supabase.table('scan_results').select("*").eq('keyword_id', keyword_id).order('created_at', desc=True).limit(5).execute()
    
    for scan in scans.data:
        scan_id = scan['id']
        
        # 2. Отримуємо бренди
        mentions_res = supabase.table('brand_mentions').select("*").eq('scan_id', scan_id).execute()
        df_brands = pd.DataFrame(mentions_res.data)
        
        # 3. Отримуємо джерела
        sources_res = supabase.table('extracted_sources').select("*").eq('scan_id', scan_id).execute()
        df_sources = pd.DataFrame(sources_res.data)
        
        # 4. Розрахунок метрик для конкретної моделі
        my_rank = None
        my_sentiment = 0
        sov = 0
        
        if not df_brands.empty:
            # Шукаємо наш бренд (шукаємо входження імені проекту в brand_name)
            my_brand_row = df_brands[df_brands['brand_name'].str.contains(project_name, case=False, na=False)]
            
            if not my_brand_row.empty:
                my_rank = my_brand_row.iloc[0]['position']
                # Нормалізуємо сентимент з -1..1 до 0..100
                raw_sent = my_brand_row.iloc[0]['sentiment']
                my_sentiment = int((raw_sent + 1) * 50) 
            
            # Рахуємо SOV (частка наших згадок)
            total_mentions = len(df_brands)
            my_mentions = len(my_brand_row)
            if total_mentions > 0:
                sov = round((my_mentions / total_mentions) * 100, 1)

        # 5. Підготовка таблиць для виводу
        display_brands = df_brands[['brand_name', 'position', 'sentiment']] if not df_brands.empty else pd.DataFrame(columns=['brand_name', 'position', 'sentiment'])
        display_sources = df_sources[['domain', 'authority_score']] if not df_sources.empty else pd.DataFrame(columns=['domain', 'authority_score'])

        # 6. Генеруємо "Mock" текст відповіді, якщо в базі немає повного тексту
        # (У майбутньому треба додати поле response_text в таблицю scan_results)
        mock_text = f"**Query Analysis:** Based on the search for citations, the market shows strong presence of {project_name}. \n\n"
        if not df_brands.empty:
            mock_text += "Key players identified:\n"
            for _, row in df_brands.iterrows():
                mock_text += f"* {row['brand_name']} (Rank #{row['position']})\n"
        
        results_list.append({
            'provider': scan['model_used'],
            'my_brand_rank': my_rank,
            'my_brand_sentiment': my_sentiment,
            'sov': sov,
            'official_sources_count': len(df_sources), # Спрощено, всі джерела вважаємо знайденими
            'raw_response_text': mock_text, 
            'brands_table': display_brands,
            'sources_table': display_sources
        })
        
    return results_list

# --- 4. PAGE VIEWS ---

def show_dashboard():
    st.title("📊 Dashboard")
    project = st.session_state['selected_project']
    if not project:
        st.warning("Please select a project.")
        return

    stats = get_dashboard_stats(project['id'])
    
    if stats:
        cols = st.columns(6)
        metrics = [
            ("SOV", f"{stats.get('sov', 0)}%", "Share of Voice"),
            ("Mentions", stats.get('absolute_counts', 0), "Vol"),
            ("Off. Src %", f"{stats.get('official_source_pct', 0)}%", "Cov"),
            ("Cit. Ratio", f"{stats.get('official_citations_ratio', 0)}", "Ref"),
            ("Sentiment", stats.get('avg_sentiment', 0), "Avg"),
            ("Position", stats.get('avg_position', 0), "Rank"),
        ]
        for col, (label, value, delta) in zip(cols, metrics):
            col.metric(label, value, delta)
    else:
        st.info("No data yet.")

    st.divider()
    
    # Simple Charts
    c1, c2 = st.columns([2, 1])
    with c1:
        st.subheader("Dynamics")
        # Mock chart
        dates = pd.date_range(end=datetime.today(), periods=14)
        fig = px.line(x=dates, y=[10, 12, 15, 14, 18, 20, 22, 21, 25, 24, 28, 30, 32, 35], template="plotly_dark", labels={'x':'Date', 'y':'SOV %'})
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.subheader("Competitors")
        # Fetch real competitor summary
        try:
            res = supabase.table('brand_mentions').select("brand_name").eq('project_id', project['id']).execute()
            if res.data:
                df = pd.DataFrame(res.data)
                counts = df['brand_name'].value_counts().reset_index().head(5)
                counts.columns = ['Brand', 'Count']
                fig_bar = px.bar(counts, x='Count', y='Brand', orientation='h', template="plotly_dark")
                st.plotly_chart(fig_bar, use_container_width=True)
        except:
            st.write("No data")

def show_keyword_tracker():
    st.title("🎯 Keyword Tracker")
    project = st.session_state['selected_project']
    project_id = project['id']
    project_name = project['name']

    # --- Section 1: Add New Keyword ---
    with st.expander("Add New Keyword"):
        with st.form("add_kw"):
            c1, c2 = st.columns([3, 1])
            kw_input = c1.text_input("Enter Keyword")
            models = c2.multiselect("Models", ["GPT-4o", "Perplexity"], default=["GPT-4o"])
            if st.form_submit_button("Add & Scan"):
                supabase.table('keywords').insert({"project_id": project_id, "keyword": kw_input, "models": models}).execute()
                st.success("Added!")
                time.sleep(1)
                st.rerun()

    # --- Section 2: Keyword List & Selection ---
    st.subheader("Monitored Keywords")
    try:
        res = supabase.table('keywords').select("*").eq('project_id', project_id).order('created_at', desc=True).execute()
        df_kw = pd.DataFrame(res.data)
        
        if df_kw.empty:
            st.info("No keywords found.")
            return

        # Selectbox for navigation
        keyword_options = {row['id']: row['keyword'] for index, row in df_kw.iterrows()}
        selected_kw_id = st.selectbox(
            "Select a keyword to analyze:", 
            options=keyword_options.keys(), 
            format_func=lambda x: keyword_options[x]
        )
        
        current_keyword = keyword_options[selected_kw_id]

        st.markdown("---")
        
        # --- Section 3: DETAILED VIEW (INTEGRATED CODE) ---
        
        # 1. Отримуємо дані з бази (підготовлені нашою функцією)
        results = get_scan_details(selected_kw_id, project_name)
        
        if not results:
            st.warning("No scan results found for this keyword yet. Try adding it again to trigger a scan.")
        else:
            st.header(f"Аналіз запиту: '{current_keyword}'")
            
            # 2. Створюємо вкладки динамічно
            tab_names = [row['provider'] for row in results]
            
            # Перевірка на дублікати імен вкладок (якщо сканували одну модель кілька разів)
            unique_tab_names = []
            for i, name in enumerate(tab_names):
                unique_tab_names.append(f"{name} ({i+1})") # Додаємо індекс для унікальності
            
            tabs = st.tabs(unique_tab_names)

            # 3. Наповнюємо кожну вкладку
            for i, tab in enumerate(tabs):
                data = results[i] 
                
                with tab:
                    # Верхня плашка з метриками
                    col1, col2, col3, col4 = st.columns(4)
                    
                    rank_display = f"#{data['my_brand_rank']}" if data['my_brand_rank'] else "Не знайдено"
                    col1.metric("Наша Позиція", rank_display)
                    
                    col2.metric("Тональність", f"{data['my_brand_sentiment']}/100")
                    col3.metric("SOV у цій відповіді", f"{data['sov']}%")
                    col4.metric("Офіційні джерела", f"{data['official_sources_count']}")
                    
                    st.divider()
                    
                    # Розділяємо екран
                    left_col, right_col = st.columns([2, 1])
                    
                    with left_col:
                        st.subheader("📝 Повна відповідь моделі")
                        st.markdown(
                            f"<div style='background-color:#262730; padding:15px; border-radius:10px;'>{data['raw_response_text']}</div>", 
                            unsafe_allow_html=True
                        )
                        
                    with right_col:
                        st.subheader("🕵️‍♂️ Аналіз сутностей")
                        
                        st.write("**Згадані Бренди:**")
                        st.dataframe(data['brands_table'], hide_index=True, use_container_width=True)
                        
                        st.write("**Знайдені Посилання:**")
                        st.dataframe(data['sources_table'], hide_index=True, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading keywords: {e}")

def show_source_intel():
    st.title("📡 Source Intelligence")
    project_id = st.session_state['selected_project']['id']
    
    # Simple list of assets
    assets = supabase.table('official_assets').select("*").eq('project_id', project_id).execute()
    st.write("### My Official Assets")
    st.dataframe(pd.DataFrame(assets.data))

def show_recommendations():
    st.title("💡 Recommendations")
    # Placeholder
    st.info("AI Strategic Agent is analyzing your latest scans...")

def show_ai_chat():
    st.title("💬 AI Analyst")
    for msg in st.session_state['chat_history']:
        st.chat_message(msg['role']).write(msg['content'])
    
    if prompt := st.chat_input():
        st.session_state['chat_history'].append({"role": "user", "content": prompt})
        st.chat_message("user").write(prompt)
        resp = "Analysis is processing..."
        st.session_state['chat_history'].append({"role": "assistant", "content": resp})
        st.chat_message("assistant").write(resp)

# --- 5. SIDEBAR & MAIN ---
def render_sidebar():
    with st.sidebar:
        user = st.session_state['user']
        st.write(f"👤 {user.email}")
        if st.button("Logout"):
            logout()
        st.divider()
        
        # Project Selector
        projects_df = get_projects(user.id, st.session_state.get('role'))
        if not projects_df.empty:
            proj_list = projects_df['name'].tolist()
            curr_idx = 0
            if st.session_state['selected_project']:
                try:
                    curr_idx = proj_list.index(st.session_state['selected_project']['name'])
                except:
                    curr_idx = 0
            sel = st.selectbox("Project", proj_list, index=curr_idx)
            st.session_state['selected_project'] = projects_df[projects_df['name']==sel].iloc[0].to_dict()
        
        return option_menu(None, ["Dashboard", "Keyword Tracker", "Source Intel", "Recommendations", "AI Chat"], 
                           icons=['graph-up', 'search', 'hdd', 'lightbulb', 'chat'], default_index=1)

def main():
    if not st.session_state['user']:
        login()
    else:
        page = render_sidebar()
        if st.session_state['selected_project']:
            if page == "Dashboard": show_dashboard()
            elif page == "Keyword Tracker": show_keyword_tracker()
            elif page == "Source Intel": show_source_intel()
            elif page == "Recommendations": show_recommendations()
            elif page == "AI Chat": show_ai_chat()
        else:
            st.info("Create a project first.")

if __name__ == "__main__":
    main()
