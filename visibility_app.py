import streamlit as st
import pandas as pd
import plotly.express as px
from supabase import create_client, Client
from streamlit_option_menu import option_menu
from datetime import datetime, timedelta
import time

# --- 1. CONFIGURATION & SETUP ---
st.set_page_config(
    page_title="AI Visibility Platform",
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

# Supabase Connection (Best practice: use st.secrets)
# For local dev, you can replace these with strings, but st.secrets is safer.
try:
    SUPABASE_URL = st.secrets["SUPABASE_URL"]
    SUPABASE_KEY = st.secrets["SUPABASE_KEY"]
except:
    # Fallback for demonstration if secrets aren't set
    SUPABASE_URL = "https://honujwuyjppukqotmfiv.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImhvbnVqd3V5anBwdWtxb3RtZml2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjUxMDc3NDIsImV4cCI6MjA4MDY4Mzc0Mn0.AX7EBrI-pHuGZTmfK5X5Ktb25FuVUTxJGwH6ikJbS-0"

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
                    
                    # Mocking Role/Balance fetch (In real app: fetch from 'profiles' table)
                    # We assume admin based on specific email for demo or metadata
                    user_role = 'admin' if 'admin' in email else 'user' 
                    st.session_state['role'] = user_role
                    st.session_state['balance'] = 1500 # Mock balance
                    
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
        st.error(f"Error fetching projects: {e}")
        return pd.DataFrame()

def get_dashboard_stats(project_id):
    try:
        # Fetching from SQL View
        response = supabase.table('dashboard_stats').select("*").eq('project_id', project_id).execute()
        if response.data:
            return response.data[0] # Return first row as dict
        return None
    except Exception as e:
        st.error(f"Error fetching stats: {e}")
        return None

# --- 4. PAGE VIEWS ---

def show_dashboard():
    st.title("📊 Dashboard")
    
    project = st.session_state['selected_project']
    if not project:
        st.warning("Please select a project from the sidebar.")
        return

    # Fetch Data
    stats = get_dashboard_stats(project['id'])
    
    # KPIs Row
    if stats:
        cols = st.columns(6)
        metrics = [
            ("SOV", f"{stats.get('sov', 0)}%", "Share of Voice"),
            ("Brand Mentions", stats.get('absolute_counts', 0), "Total Volume"),
            ("Official Src %", f"{stats.get('official_source_pct', 0)}%", "Asset Coverage"),
            ("Citations Ratio", f"{stats.get('official_citations_ratio', 0)}", "Ref Quality"),
            ("Avg Sentiment", stats.get('avg_sentiment', 0), "-1 to 1"),
            ("Avg Position", stats.get('avg_position', 0), "Rank"),
        ]
        
        for col, (label, value, delta) in zip(cols, metrics):
            col.metric(label, value, delta)
    else:
        st.info("No scan data available for this project yet.")

    st.divider()

    # Charts
    c1, c2 = st.columns([2, 1])
    
    with c1:
        st.subheader("SOV Dynamics")
        # Mocking time-series data since view provides snapshot
        # In production: Fetch from 'scan_results' with timestamps
        dates = pd.date_range(start=datetime.today() - timedelta(days=30), periods=30)
        import numpy as np
        mock_sov = np.random.uniform(10, 30, size=30)
        df_sov = pd.DataFrame({'Date': dates, 'SOV': mock_sov})
        
        fig_sov = px.line(df_sov, x='Date', y='SOV', markers=True, template="plotly_dark")
        fig_sov.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig_sov, use_container_width=True)

    with c2:
        st.subheader("Top Competitors")
        try:
            # Fetch real competitor data
            res = supabase.table('brand_mentions').select("brand_name, count").eq('project_id', project['id']).order('count', desc=True).limit(5).execute()
            df_comp = pd.DataFrame(res.data) if res.data else pd.DataFrame({'brand_name': ['Competitor A', 'Competitor B'], 'count': [45, 30]})
            
            if not df_comp.empty:
                fig_bar = px.bar(df_comp, x='count', y='brand_name', orientation='h', template="plotly_dark", color='count')
                fig_bar.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20))
                st.plotly_chart(fig_bar, use_container_width=True)
        except Exception:
            st.error("Could not load competitor data.")

def show_keyword_tracker():
    st.title("🎯 Keyword Tracker")
    project_id = st.session_state['selected_project']['id']

    # Input Form
    with st.expander("Add New Keyword", expanded=True):
        with st.form("add_keyword"):
            c1, c2 = st.columns([3, 1])
            kw_input = c1.text_input("Enter Query/Keyword")
            models = c2.multiselect("Target Models", ["GPT-4o", "Perplexity", "Claude 3.5"], default=["GPT-4o"])
            submitted = st.form_submit_button("Add & Scan 🚀")
            
            if submitted and kw_input:
                try:
                    # 1. Insert into DB
                    data = {"project_id": project_id, "keyword": kw_input, "models": models}
                    supabase.table('keywords').insert(data).execute()
                    # 2. Mock Webhook/Trigger Scan
                    st.toast(f"Scanning '{kw_input}' on {len(models)} models...", icon="⏳")
                    time.sleep(1) 
                    st.toast("Scan initiated!", icon="✅")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error adding keyword: {e}")

    # Keywords Table
    st.subheader("Monitored Keywords")
    try:
        res = supabase.table('keywords').select("*").eq('project_id', project_id).order('created_at', desc=True).execute()
        df_kw = pd.DataFrame(res.data)
        
        if not df_kw.empty:
            # Master-Detail view using Selectbox instead of full drill-down for simplicity
            selected_kw_id = st.selectbox("Select Keyword to View Details", options=df_kw['id'], format_func=lambda x: df_kw[df_kw['id']==x]['keyword'].values[0])
            
            st.markdown("---")
            st.write(f"**Drill-down for:** `{df_kw[df_kw['id']==selected_kw_id]['keyword'].values[0]}`")
            
            t1, t2, t3 = st.tabs(["Response Text", "Brand Mentions", "Sources"])
            
            with t1:
                st.info("Mocked Response: 'The best options for CRM systems in 2024 include Salesforce, HubSpot, and [Your Brand]...'")
            with t2:
                st.dataframe(pd.DataFrame({"Brand": ["Salesforce", "Your Brand"], "Position": [1, 3], "Sentiment": ["Positive", "Neutral"]}))
            with t3:
                st.dataframe(pd.DataFrame({"Source Domain": ["g2.com", "capterra.com"], "Authority": [90, 85]}))

        else:
            st.info("No keywords found. Add one above.")
            
    except Exception as e:
        st.error(f"Error loading keywords: {e}")

def show_source_intel():
    st.title("📡 Source Intelligence")
    project_id = st.session_state['selected_project']['id']
    
    tab1, tab2 = st.tabs(["My Assets", "Market Analysis"])
    
    with tab1:
        st.subheader("Official Brand Assets")
        c1, c2 = st.columns([3, 1])
        new_domain = c1.text_input("Add Official Domain (e.g., yoursite.com)")
        if c2.button("Add Asset"):
            try:
                supabase.table('official_assets').insert({"project_id": project_id, "domain": new_domain}).execute()
                st.success("Added!")
                st.rerun()
            except Exception as e:
                st.error(str(e))
        
        # List assets
        assets_res = supabase.table('official_assets').select("*").eq('project_id', project_id).execute()
        st.dataframe(pd.DataFrame(assets_res.data), use_container_width=True)

    with tab2:
        st.subheader("Source Landscape")
        try:
            # Fetch extracted sources (Mocking grouping logic if SQL view isn't ready)
            sources_res = supabase.table('extracted_sources').select("domain").eq('project_id', project_id).execute()
            if sources_res.data:
                df_src = pd.DataFrame(sources_res.data)
                df_grouped = df_src['domain'].value_counts().reset_index()
                df_grouped.columns = ['Domain', 'Citations']
                
                # Check intersection with assets
                assets = [a['domain'] for a in assets_res.data] if assets_res.data else []
                
                def highlight_owned(val):
                    color = '#1b5e20' if val in assets else '' # Green for owned
                    return f'background-color: {color}'

                st.dataframe(df_grouped.style.applymap(highlight_owned, subset=['Domain']), use_container_width=True)
            else:
                st.info("No sources extracted yet.")
        except Exception as e:
            st.error(f"Analysis error: {e}")

def show_recommendations():
    st.title("💡 Strategic Recommendations")
    
    # Admin Controls
    if st.session_state.get('role') == 'admin':
        st.write("### 🛠 Admin Generator")
        cols = st.columns(4)
        types = ["Digital", "PR", "Content", "Social"]
        for i, t in enumerate(types):
            if cols[i].button(f"Generate {t}"):
                st.toast(f"Generating {t} Report via LLM Agent...", icon="🤖")
                time.sleep(2)
                st.success("Report Generated & Saved to DB")
    
    st.divider()
    
    # View Reports
    st.subheader("Available Reports")
    try:
        project_id = st.session_state['selected_project']['id']
        reports = supabase.table('recommendation_reports').select("*").eq('project_id', project_id).order('created_at', desc=True).execute()
        
        if reports.data:
            for report in reports.data:
                with st.expander(f"📄 {report.get('type', 'General')} Report - {report.get('created_at')[:10]}"):
                    st.markdown(report.get('content', 'No content.'))
        else:
            st.info("No recommendations generated yet.")
    except Exception as e:
        st.error(f"Error loading reports: {e}")

def show_ai_chat():
    st.title("💬 GEO-Analyst AI Assistant")
    
    for msg in st.session_state['chat_history']:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])
            
    if prompt := st.chat_input("Ask about your brand visibility..."):
        # User message
        st.session_state['chat_history'].append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.write(prompt)
            
        # Assistant response (Mock)
        response_text = f"Based on the analysis of project **{st.session_state['selected_project']['name']}**, '{prompt}' is an interesting point. Your SOV is currently stable."
        
        st.session_state['chat_history'].append({"role": "assistant", "content": response_text})
        with st.chat_message("assistant"):
            st.write(response_text)

# --- 5. SIDEBAR & NAVIGATION ---
def render_sidebar():
    with st.sidebar:
        # Profile Section
        user = st.session_state['user']
        st.write(f"👤 **{user.email}**")
        st.caption(f"Role: {st.session_state.get('role', 'User').upper()} | Credits: {st.session_state.get('balance', 0)}")
        
        if st.button("Logout", key="logout_btn"):
            logout()
            
        st.divider()
        
        # Project Selector
        projects_df = get_projects(user.id, st.session_state.get('role'))
        if not projects_df.empty:
            proj_list = projects_df['name'].tolist()
            # Find index of currently selected project
            current_idx = 0
            if st.session_state['selected_project']:
                try:
                    current_idx = proj_list.index(st.session_state['selected_project']['name'])
                except ValueError:
                    current_idx = 0
            
            selected_proj_name = st.selectbox("Select Project", proj_list, index=current_idx)
            
            # Update session state
            st.session_state['selected_project'] = projects_df[projects_df['name'] == selected_proj_name].iloc[0].to_dict()
        else:
            st.warning("No projects found.")
            st.session_state['selected_project'] = None

        st.divider()
        
        # Navigation Menu
        selected = option_menu(
            menu_title=None,
            options=["Dashboard", "Keyword Tracker", "Source Intel", "Recommendations", "AI Chat"],
            icons=["speedometer2", "search", "hdd-network", "lightbulb", "chat-dots"],
            default_index=0,
            styles={
                "nav-link": {"font-size": "14px", "text-align": "left", "margin": "0px"},
                "nav-link-selected": {"background-color": "#FF4B4B"},
            }
        )
        return selected

# --- 6. MAIN APP LOOP ---
def main():
    if not st.session_state['user']:
        login()
    else:
        page = render_sidebar()
        
        if st.session_state['selected_project']:
            if page == "Dashboard":
                show_dashboard()
            elif page == "Keyword Tracker":
                show_keyword_tracker()
            elif page == "Source Intel":
                show_source_intel()
            elif page == "Recommendations":
                show_recommendations()
            elif page == "AI Chat":
                show_ai_chat()
        else:
            st.info("👈 Please create or select a project in the sidebar to begin.")

if __name__ == "__main__":
    main()
