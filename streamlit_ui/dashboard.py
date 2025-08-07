import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="🌍 Air Quality Monitoring Dashboard",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_resource
def create_spark_session():
    """Create Spark session with caching"""
    try:
        spark = SparkSession.builder \
            .appName("AirQualityDashboard") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as e:
        st.error(f"❌ Failed to create Spark session: {e}")
        return None

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data(data_path):
    """Load data from HDFS with caching"""
    spark = create_spark_session()
    if not spark:
        return None
    
    try:
        # Try to read from processed data first
        try:
            df = spark.read.parquet(f"{data_path}_processed")
            st.success("✅ Loaded processed data from HDFS")
        except:
            # Fallback to raw JSON data
            df = spark.read.json(data_path)
            st.info("ℹ️ Loaded raw data from HDFS")
        
        if df.count() == 0:
            st.warning("⚠️ No data available in HDFS")
            return None
        
        # Convert to Pandas for Streamlit/Plotly
        pandas_df = df.toPandas()
        return pandas_df
        
    except Exception as e:
        st.error(f"❌ Error loading data: {e}")
        return None

def create_air_quality_gauge(aqi_value, pm25_value):
    """Create air quality gauge chart"""
    fig = go.Figure(go.Indicator(
        mode = "gauge+number+delta",
        value = pm25_value,
        domain = {'x': [0, 1], 'y': [0, 1]},
        title = {'text': "PM2.5 (µg/m³)"},
        delta = {'reference': 35},
        gauge = {
            'axis': {'range': [None, 100]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 12], 'color': "lightgreen"},
                {'range': [12, 35], 'color': "yellow"},
                {'range': [35, 55], 'color': "orange"},
                {'range': [55, 100], 'color': "red"}],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 90}}))
    
    fig.update_layout(height=300)
    return fig

def create_time_series_chart(df, metric):
    """Create time series chart for a specific metric"""
    if 'timestamp' not in df.columns:
        st.warning(f"⚠️ No timestamp column found for time series")
        return None
    
    try:
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df_sorted = df.sort_values('timestamp')
        
        fig = px.line(df_sorted, 
                     x='timestamp', 
                     y=metric,
                     color='location' if 'location' in df.columns else None,
                     title=f'{metric.upper()} Over Time')
        
        fig.update_layout(
            xaxis_title="Time",
            yaxis_title=metric.upper(),
            hovermode='x unified'
        )
        
        return fig
    except Exception as e:
        st.error(f"❌ Error creating time series chart: {e}")
        return None

def create_pollutant_comparison(df):
    """Create pollutant comparison chart"""
    pollutants = ['pm2_5', 'pm10', 'no2', 'o3', 'co', 'so2']
    available_pollutants = [p for p in pollutants if p in df.columns]
    
    if not available_pollutants:
        st.warning("⚠️ No pollutant data available")
        return None
    
    try:
        # Calculate average values for each pollutant
        avg_values = []
        for pollutant in available_pollutants:
            avg_val = df[pollutant].mean()
            avg_values.append(avg_val if pd.notna(avg_val) else 0)
        
        fig = go.Figure(data=[
            go.Bar(x=available_pollutants, y=avg_values, 
                  marker_color=['red', 'orange', 'yellow', 'green', 'blue', 'purple'][:len(available_pollutants)])
        ])
        
        fig.update_layout(
            title="Average Pollutant Levels",
            xaxis_title="Pollutants",
            yaxis_title="Concentration (µg/m³)"
        )
        
        return fig
    except Exception as e:
        st.error(f"❌ Error creating pollutant comparison: {e}")
        return None

def create_temperature_humidity_scatter(df):
    """Create temperature vs humidity scatter plot"""
    if 'temp_c' not in df.columns or 'humidity' not in df.columns:
        st.warning("⚠️ Temperature or humidity data not available")
        return None
    
    try:
        fig = px.scatter(df, 
                        x='temp_c', 
                        y='humidity',
                        color='location' if 'location' in df.columns else None,
                        size='pm2_5' if 'pm2_5' in df.columns else None,
                        title="Temperature vs Humidity",
                        labels={
                            'temp_c': 'Temperature (°C)',
                            'humidity': 'Humidity (%)'
                        })
        
        return fig
    except Exception as e:
        st.error(f"❌ Error creating scatter plot: {e}")
        return None

def display_summary_stats(df):
    """Display summary statistics"""
    st.subheader("📊 Summary Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if 'location' in df.columns:
            unique_locations = df['location'].nunique()
            st.metric("🏙️ Locations", unique_locations)
        else:
            st.metric("🏙️ Locations", "N/A")
    
    with col2:
        total_records = len(df)
        st.metric("📝 Total Records", total_records)
    
    with col3:
        if 'temp_c' in df.columns:
            avg_temp = df['temp_c'].mean()
            st.metric("🌡️ Avg Temperature", f"{avg_temp:.1f}°C")
        else:
            st.metric("🌡️ Avg Temperature", "N/A")
    
    with col4:
        if 'pm2_5' in df.columns:
            avg_pm25 = df['pm2_5'].mean()
            st.metric("💨 Avg PM2.5", f"{avg_pm25:.1f} µg/m³")
        else:
            st.metric("💨 Avg PM2.5", "N/A")

def main():
    """Main dashboard function"""
    st.title("🌍 Real-Time Air Quality Monitoring Dashboard")
    st.markdown("---")
    
    # Sidebar configuration
    st.sidebar.header("⚙️ Configuration")
    
    # Data source selection
    hdfs_path = st.sidebar.text_input(
        "HDFS Data Path", 
        value="hdfs://localhost:9000/user/sunbeam/air_quality",
        help="Path to your HDFS data directory"
    )
    
    # Auto-refresh option
    auto_refresh = st.sidebar.checkbox("🔄 Auto Refresh (30s)", value=False)
    
    if auto_refresh:
        st.rerun()
    
    # Manual refresh button
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    
    # Load data
    with st.spinner("📥 Loading data from HDFS..."):
        df = load_data(hdfs_path)
    
    if df is None or df.empty:
        st.error("❌ No data available. Please check your HDFS path and ensure data is being produced.")
        st.info("💡 Make sure your producer and consumer scripts are running!")
        
        # Show sample data structure
        st.subheader("📋 Expected Data Structure")
        sample_data = {
            'location': ['London', 'London'],
            'temp_c': [15.5, 16.2],
            'humidity': [65, 68],
            'pm2_5': [12.3, 14.1],
            'pm10': [18.5, 20.2],
            'no2': [25.1, 23.8],
            'timestamp': ['2024-01-01T12:00:00', '2024-01-01T12:10:00']
        }
        st.json(sample_data)
        return
    
    # Display summary statistics
    display_summary_stats(df)
    
    # Create tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "📈 Time Series", "🌡️ Environmental", "🔍 Raw Data"])
    
    with tab1:
        st.subheader("📊 Air Quality Overview")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Air Quality Gauge
            if 'pm2_5' in df.columns:
                current_pm25 = df['pm2_5'].iloc[-1] if len(df) > 0 else 0
                gauge_fig = create_air_quality_gauge(0, current_pm25)
                st.plotly_chart(gauge_fig, use_container_width=True)
            else:
                st.warning("⚠️ PM2.5 data not available for gauge")
        
        with col2:
            # Pollutant Comparison
            pollutant_fig = create_pollutant_comparison(df)
            if pollutant_fig:
                st.plotly_chart(pollutant_fig, use_container_width=True)
        
        # Air Quality Index Distribution (if available)
        if 'air_quality_index' in df.columns:
            st.subheader("🌬️ Air Quality Index Distribution")
            aqi_counts = df['air_quality_index'].value_counts()
            fig_aqi = px.pie(values=aqi_counts.values, names=aqi_counts.index, 
                           title="Air Quality Categories")
            st.plotly_chart(fig_aqi, use_container_width=True)
    
    with tab2:
        st.subheader("📈 Time Series Analysis")
        
        # Metric selection
        numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns.tolist()
        if numeric_columns:
            selected_metric = st.selectbox(
                "Select Metric for Time Series",
                options=numeric_columns,
                index=0 if 'pm2_5' not in numeric_columns else numeric_columns.index('pm2_5')
            )
            
            # Create time series chart
            ts_fig = create_time_series_chart(df, selected_metric)
            if ts_fig:
                st.plotly_chart(ts_fig, use_container_width=True)
        else:
            st.warning("⚠️ No numeric columns available for time series")
    
    with tab3:
        st.subheader("🌡️ Environmental Conditions")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Temperature vs Humidity scatter
            scatter_fig = create_temperature_humidity_scatter(df)
            if scatter_fig:
                st.plotly_chart(scatter_fig, use_container_width=True)
        
        with col2:
            # Temperature distribution
            if 'temp_c' in df.columns:
                fig_temp = px.histogram(df, x='temp_c', title="Temperature Distribution")
                st.plotly_chart(fig_temp, use_container_width=True)
            else:
                st.warning("⚠️ Temperature data not available")
        
        # Humidity analysis
        if 'humidity' in df.columns:
            st.subheader("💧 Humidity Analysis")
            col3, col4 = st.columns(2)
            
            with col3:
                avg_humidity = df['humidity'].mean()
                st.metric("Average Humidity", f"{avg_humidity:.1f}%")
            
            with col4:
                humidity_range = df['humidity'].max() - df['humidity'].min()
                st.metric("Humidity Range", f"{humidity_range:.1f}%")
    
    with tab4:
        st.subheader("🔍 Raw Data")
        
        # Display options
        col1, col2 = st.columns(2)
        with col1:
            num_rows = st.number_input("Number of rows to display", min_value=1, max_value=1000, value=50)
        with col2:
            sort_by = st.selectbox("Sort by", options=df.columns.tolist(), 
                                 index=0 if 'timestamp' not in df.columns else df.columns.tolist().index('timestamp'))
        
        # Filter options
        if 'location' in df.columns:
            locations = df['location'].unique().tolist()
            selected_locations = st.multiselect("Filter by Location", options=locations, default=locations)
            df_filtered = df[df['location'].isin(selected_locations)]
        else:
            df_filtered = df
        
        # Display filtered and sorted data
        df_display = df_filtered.sort_values(by=sort_by, ascending=False).head(num_rows)
        st.dataframe(df_display, use_container_width=True)
        
        # Download button
        csv = df_display.to_csv(index=False)
        st.download_button(
            label="📥 Download Data as CSV",
            data=csv,
            file_name=f"air_quality_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center'>
            <p>🌍 Air Quality Monitoring System | Last Updated: {}</p>
        </div>
        """.format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
