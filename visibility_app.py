"""
GEO-Analyst SaaS Platform
-------------------------
requirements.txt:
streamlit
supabase
pandas
plotly
streamlit-option-menu
requests
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from supabase import create_client, Client
from datetime import datetime
import requests
import time
from streamlit_option_menu import option_menu

# --- 1. CONFIGURATION & SETUP ---
st.set_page_config(
    page_title="GEO-Analyst Platform",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
<style>
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #e0e0e0;
    }
    .stButton>button {
        width: 100%;
        border-radius: 5px;
    }
    div[data-testid="stExpander"] details summary p {
        font-weight: 600;
        font-size: 1.1em;
    }
</style>
""", unsafe_allow_html=True)

# --- 2. SUPABASE CONNECTION ---
@st.cache_resource
def init_supabase() -> Client:
    try:
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
        return create_client(url, key)
    except Exception as e:
        st.error(f"Failed to connect to Supabase: {e}")
        st.stop()

supabase = init_supabase()

# --- 3. AUTHENTICATION HELPERS ---
def login_user(email, password):
    try:
        response = supabase.auth.sign_in_with_password({"email": email, "password": password})
        st.session_state["user"] = response.user
        st.session_state["access_token"] = response.session.access_token
        st.rerun()
    except Exception as e:
        st.error(f"Login failed: {e}")

def logout_user():
    supabase.auth.sign_out()
    st.session_state.clear()
    st.rerun()

def get_user_profile(user_id):
    try:
        response = supabase.table("profiles").select("*").eq("id", user_id).execute()
        if response.data:
            return response.data[0]
    except Exception:
        return None
    return {"role": "user", "balance": 0.0}

# --- 4. PAGE FUNCTIONS ---

def render_dashboard(project_id):
    st.title("📊 Executive Dashboard")
    
    # A. Fetch KPI Data from SQL View
    try:
        # Note: We filter the SQL view by project_id
        stats_response = supabase.table("dashboard_stats").select("*").eq("project_id", project_id).execute()
        
        if not stats_response.data:
            st.info("No dashboard data available yet. Please run a scan.")
            return

        data = stats_response.data[0] # Assuming one row per project in view
        
        # Unpack JSON metrics safely
        mentions_ratio = data.get('mentions_ratio', {'my': 0, 'total': 0})
        sources_ratio = data.get('sources_ratio', {'my': 0, 'total': 0})

        # KPI Metrics
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        col1.metric("SOV %", f"{data.get('sov', 0)}%")
        col2.metric("Official Sources", f"{data.get('official_source_pct', 0)}%")
        col3.metric("Avg Sentiment", f"{data.get('avg_sentiment', 0)}")
        col4.metric("Mentions (My/Total)", f"{mentions_ratio.get('my',0)} / {mentions_ratio.get('total',0)}")
        col5.metric("Sources (My/Total)", f"{sources_ratio.get('my',0)} / {sources_ratio.get('total',0)}")
        col6.metric("Avg Rank", f"{data.get('avg_position', 0)}")

        st.markdown("---")

        # B. Charts Section
        c1, c2 = st.columns([2, 1])

        with c1:
            st.subheader("Share of Voice (SOV) Trend")
            # Query: Join scan_results and brand_mentions to get trend
            # Note: Supabase-py join syntax relies on foreign keys setup in DB
            trend_data = supabase.table("brand_mentions").select(
                "created_at, sentiment_score, scan_results(created_at)"
            ).eq("is_my_brand", True).execute()
            
            # Since join syntax can be complex in client, we simulate fetching scan dates
            # Ideally, use a stored procedure or view for time-series data.
            # Fallback: Fetch scan results for this project
            scans = supabase.table("scan_results").select("id, created_at").eq("project_id", project_id).order("created_at").execute()
            
            if scans.data:
                scan_df = pd.DataFrame(scans.data)
                # Mocking SOV calculation per scan for visualization (replace with real aggregate query)
                scan_df['sov_mock'] = [data.get('sov', 0)] * len(scan_df) 
                
                fig_trend = px.line(scan_df, x='created_at', y='sov_mock', title="SOV Over Time", markers=True)
                fig_trend.update_layout(height=350)
                st.plotly_chart(fig_trend, use_container_width=True)
            else:
                st.info("Not enough data for trend analysis.")

        with c2:
            st.subheader("Top Competitors")
            # Fetch mentions where is_my_brand is False
            comps = supabase.table("brand_mentions")\
                .select("brand_name, mention_count")\
                .eq("is_my_brand", False)\
                .execute()
            
            if comps.data:
                comp_df = pd.DataFrame(comps.data)
                comp_agg = comp_df.groupby("brand_name")['mention_count'].sum().reset_index()
                comp_agg = comp_agg.sort_values(by="mention_count", ascending=False).head(5)
                
                fig_bar = px.bar(comp_agg, x="mention_count", y="brand_name", orientation='h', color="brand_name")
                fig_bar.update_layout(height=350, showlegend=False)
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.caption("No competitor data found.")

    except Exception as e:
        st.error(f"Error loading dashboard: {e}")

def render_keyword_tracker(project_id):
    st.title("🔎 Keyword Tracker")
    
    # 1. Add New Keyword Form
    with st.expander("Add New Keyword / Run Scan", expanded=True):
        with st.form("new_scan_form"):
            c1, c2 = st.columns([3, 1])
            with c1:
                keyword_input = st.text_input("Enter Keyword or Question", placeholder="e.g., Best CRM for small business")
            with c2:
                models = st.multiselect("Select Models", ["Perplexity", "GPT-4o", "Claude-3.5"], default=["Perplexity"])
            
            submitted = st.form_submit_button("Add & Scan")
            
            if submitted and keyword_input:
                try:
                    # Insert into scan_results
                    new_scan = {
                        "project_id": project_id,
                        "keyword_text": keyword_input,
                        "provider": models[0], # Primary provider
                        "created_at": datetime.now().isoformat()
                    }
                    res = supabase.table("scan_results").insert(new_scan).execute()
                    
                    if res.data:
                        # Trigger n8n Webhook
                        webhook_url = st.secrets.get("N8N_WEBHOOK_URL", "")
                        if webhook_url:
                            requests.post(webhook_url, json={
                                "scan_id": res.data[0]['id'],
                                "keyword": keyword_input,
                                "models": models,
                                "project_id": project_id
                            })
                            st.success("Scan initiated! Check back shortly.")
                        else:
                            st.warning("Scan saved, but Webhook URL is missing in secrets.")
                    else:
                        st.error("Failed to save scan.")
                        
                except Exception as e:
                    st.error(f"Error initiating scan: {e}")

    st.divider()

    # 2. Tracked Keywords List
    try:
        scans = supabase.table("scan_results").select("*").eq("project_id", project_id).order("created_at", desc=True).execute()
        
        if not scans.data:
            st.info("No keywords tracked yet.")
            return

        df_scans = pd.DataFrame(scans.data)
        
        # Display as a clean dataframe or metric cards
        # We will use an interactive selection mechanism
        
        st.subheader("Recent Scans")
        
        # Create a display DF
        display_df = df_scans[['keyword_text', 'provider', 'created_at']].copy()
        display_df['created_at'] = pd.to_datetime(display_df['created_at']).dt.strftime('%Y-%m-%d %H:%M')
        
        col_list, col_detail = st.columns([1, 2])
        
        with col_list:
            selected_id = st.radio(
                "Select a scan to view details:",
                options=df_scans['id'].tolist(),
                format_func=lambda x: df_scans[df_scans['id'] == x]['keyword_text'].values[0]
            )

        # 3. Drill Down Details
        if selected_id:
            with col_detail:
                scan_detail = df_scans[df_scans['id'] == selected_id].iloc[0]
                st.markdown(f"### Results for: *{scan_detail['keyword_text']}*")
                st.caption(f"Scanned on: {scan_detail['created_at']} via {scan_detail['provider']}")
                
                tab_brands, tab_sources = st.tabs(["Detected Brands", "Extracted Sources"])
                
                with tab_brands:
                    # Fetch brand mentions for this scan
                    mentions = supabase.table("brand_mentions").select("*").eq("scan_result_id", selected_id).execute()
                    if mentions.data:
                        m_df = pd.DataFrame(mentions.data)
                        st.dataframe(
                            m_df[['brand_name', 'rank_position', 'sentiment_score', 'is_my_brand']],
                            use_container_width=True,
                            hide_index=True
                        )
                    else:
                        st.warning("Processing or no brands found.")

                with tab_sources:
                    # Fetch sources for this scan
                    sources = supabase.table("extracted_sources").select("*").eq("scan_result_id", selected_id).execute()
                    if sources.data:
                        s_df = pd.DataFrame(sources.data)
                        st.dataframe(
                            s_df[['domain', 'url', 'is_official']],
                            use_container_width=True,
                            hide_index=True,
                            column_config={"url": st.column_config.LinkColumn("Link")}
                        )
                    else:
                        st.warning("No sources extracted.")

    except Exception as e:
        st.error(f"Error fetching data: {e}")

def render_source_intel(project_id):
    st.title("📡 Source Intelligence")
    
    tab1, tab2 = st.tabs(["My Official Assets", "Market Analysis"])
    
    # TAB 1: CRUD Official Assets
    with tab1:
        st.subheader("Manage Official Domains & Socials")
        st.caption("These assets are used to calculate 'Official Source %'")
        
        # Add new
        c1, c2 = st.columns([3, 1])
        new_asset = c1.text_input("Add Domain or URL (e.g., twitter.com/mybrand)")
        if c2.button("Add Asset"):
            if new_asset:
                try:
                    supabase.table("official_assets").insert({"project_id": project_id, "domain_or_url": new_asset}).execute()
                    st.success("Asset added.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")
        
        # List and Delete
        try:
            assets = supabase.table("official_assets").select("*").eq("project_id", project_id).execute()
            if assets.data:
                for asset in assets.data:
                    c_row1, c_row2 = st.columns([4, 1])
                    c_row1.text(asset['domain_or_url'])
                    if c_row2.button("Delete", key=f"del_{asset['id']}"):
                        supabase.table("official_assets").delete().eq("id", asset['id']).execute()
                        st.rerun()
            else:
                st.info("No official assets defined.")
        except Exception as e:
            st.error(f"Error loading assets: {e}")

    # TAB 2: Market Analysis
    with tab2:
        st.subheader("Top Domains Appearing in Search")
        try:
            # Join logic: get scan_results for project -> extracted_sources
            # Simulating join by fetching all sources for project's scans (This is resource intensive in production, better use SQL View)
            # For this demo, we assume we fetch sources linked to scan_results of this project
            
            # Step 1: Get Scan IDs
            scans = supabase.table("scan_results").select("id").eq("project_id", project_id).execute()
            scan_ids = [s['id'] for s in scans.data]
            
            if scan_ids:
                # Step 2: Get Sources
                sources_resp = supabase.table("extracted_sources").select("*").in_("scan_result_id", scan_ids).execute()
                
                if sources_resp.data:
                    df = pd.DataFrame(sources_resp.data)
                    
                    # Group by domain
                    domain_stats = df['domain'].value_counts().reset_index()
                    domain_stats.columns = ['domain', 'count']
                    domain_stats = domain_stats.head(15)
                    
                    # Check official status (simple check against list from Tab 1)
                    # Ideally this is done in SQL
                    official_assets_resp = supabase.table("official_assets").select("domain_or_url").eq("project_id", project_id).execute()
                    official_list = [x['domain_or_url'] for x in official_assets_resp.data]
                    
                    domain_stats['type'] = domain_stats['domain'].apply(lambda x: 'Official' if any(o in x for o in official_list) else 'External')
                    
                    fig = px.bar(
                        domain_stats, 
                        x='count', 
                        y='domain', 
                        color='type',
                        color_discrete_map={'Official': '#00CC96', 'External': '#636EFA'},
                        orientation='h',
                        title="Most Frequent Sources"
                    )
                    fig.update_layout(yaxis={'categoryorder':'total ascending'})
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No sources analyzed yet.")
            else:
                st.info("Run a scan to see source data.")

        except Exception as e:
            st.error(f"Analysis failed: {e}")

def render_recommendations(project_id, user_role):
    st.title("💡 Strategic Recommendations")
    
    # Admin Controls
    if user_role == 'admin':
        with st.expander("Admin: Generate Reports", expanded=False):
            c1, c2, c3, c4 = st.columns(4)
            if c1.button("Generate Digital"):
                trigger_report(project_id, "digital")
            if c2.button("Generate PR"):
                trigger_report(project_id, "pr")
            if c3.button("Generate Tech"):
                trigger_report(project_id, "tech")
            if c4.button("Generate Full"):
                trigger_report(project_id, "full")

    # Display Reports
    try:
        reports = supabase.table("recommendation_reports").select("*").eq("project_id", project_id).order("created_at", desc=True).execute()
        
        if reports.data:
            for report in reports.data:
                with st.expander(f"{report['report_type'].upper()} Report - {report['created_at'][:10]}"):
                    st.markdown(report['report_content'])
        else:
            st.info("No recommendations generated yet.")
            
    except Exception as e:
        st.error(f"Error loading reports: {e}")

def trigger_report(pid, r_type):
    url = st.secrets.get("N8N_REC_WEBHOOK", "")
    if url:
        try:
            requests.post(url, json={"project_id": pid, "type": r_type})
            st.success(f"{r_type.capitalize()} report generation triggered!")
        except Exception as e:
            st.error(f"Failed to trigger: {e}")
    else:
        st.warning("Webhook URL missing.")

def render_ai_chat(project_id):
    st.title("🤖 Virshi AI Assistant")
    
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # User Input
    if prompt := st.chat_input("Ask about your GEO data..."):
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Mock Response (In production, connect to OpenAI/LangChain here)
        with st.chat_message("assistant"):
            response_text = f"I analyzed the data for project ID {project_id}. Regarding '{prompt}', I see a positive trend in your Share of Voice on Perplexity, but GPT-4 is still recommending your competitor."
            st.markdown(response_text)
            st.session_state.messages.append({"role": "assistant", "content": response_text})

# --- 5. MAIN APP LAYOUT ---

def main():
    # A. Authentication Check
    if "user" not in st.session_state:
        # Login Page
        c1, c2, c3 = st.columns([1, 2, 1])
        with c2:
            st.title("🔐 GEO-Analyst Login")
            with st.form("login_form"):
                email = st.text_input("Email")
                password = st.text_input("Password", type="password")
                submit = st.form_submit_button("Sign In")
                
                if submit:
                    login_user(email, password)
        return

    # B. App Logic (Logged In)
    user = st.session_state["user"]
    
    # Fetch Profile
    if "profile" not in st.session_state:
        st.session_state["profile"] = get_user_profile(user.id)
    
    profile = st.session_state["profile"]
    
    # Sidebar
    with st.sidebar:
        st.image("https://placehold.co/200x60/png?text=GEO-Analyst", use_container_width=True) # Placeholder Logo
        st.write(f"**{user.email}**")
        st.caption(f"Role: {profile.get('role', 'user')} | Credits: ${profile.get('balance', 0)}")
        
        st.divider()
        
        # Project Selector
        try:
            if profile.get('role') == 'admin':
                projs = supabase.table("projects").select("id, brand_name").execute()
            else:
                projs = supabase.table("projects").select("id, brand_name").eq("user_id", user.id).execute()
            
            project_options = {p['brand_name']: p['id'] for p in projs.data} if projs.data else {}
            
            if not project_options:
                st.warning("No projects found.")
                selected_project_name = None
            else:
                selected_project_name = st.selectbox("Select Project", list(project_options.keys()))
                st.session_state['selected_project_id'] = project_options[selected_project_name]

        except Exception as e:
            st.error("Error fetching projects")
            selected_project_name = None

        st.divider()

        # Navigation
        selected_page = option_menu(
            "Menu",
            ["Dashboard", "Keyword Tracker", "Source Intel", "Recommendations", "AI Chat"],
            icons=['speedometer2', 'search', 'diagram-3', 'lightbulb', 'robot'],
            menu_icon="cast",
            default_index=0,
            styles={
                "nav-link-selected": {"background-color": "#ff4b4b"},
            }
        )
        
        st.divider()
        if st.button("Logout"):
            logout_user()

    # C. Main Content Area
    if 'selected_project_id' in st.session_state and st.session_state['selected_project_id']:
        pid = st.session_state['selected_project_id']
        
        if selected_page == "Dashboard":
            render_dashboard(pid)
        elif selected_page == "Keyword Tracker":
            render_keyword_tracker(pid)
        elif selected_page == "Source Intel":
            render_source_intel(pid)
        elif selected_page == "Recommendations":
            render_recommendations(pid, profile.get('role'))
        elif selected_page == "AI Chat":
            render_ai_chat(pid)
    else:
        st.warning("Please create or select a project to continue.")

if __name__ == "__main__":
    main()
