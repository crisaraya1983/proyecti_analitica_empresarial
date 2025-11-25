import streamlit as st
import pandas as pd

from modulos.componentes import inicializar_componentes

# CONFIGURACIÓN DE STREAMLIT

def configurar_pagina():
    st.set_page_config(
        page_title="Banco Horizonte - Análisis Geoespacial",
        page_icon="🏦",
        layout="wide",
        initial_sidebar_state="expanded"
    )


def aplicar_estilos():
    st.markdown("""
    <style>
    /* Paleta de colores y variables */
    :root {
        --color-primary: #1a365d;
        --color-secondary: #2c5aa0;
        --color-accent: #f7fafc;
        --color-text: #2d3748;
        --color-border: #e2e8f0;
    }
    
    .main-header {
        color: #1a365d;
        font-size: 2em;
        font-weight: 600;
        margin-bottom: 0.5em;
        margin-top: 0em;
        letter-spacing: 0.5px;
    }
    
    .section-header {
        color: #2d3748;
        font-size: 0.85em;
        font-weight: 700;
        margin-top: 1.5em;
        margin-bottom: 1em;
        text-transform: uppercase;
        letter-spacing: 0.8px;
        color: #718096;
    }
    
    .metric-box {
        background: #f8f9fa;
        padding: 20px;
        border-radius: 8px;
        border-left: 4px solid #2c5aa0;
        margin-bottom: 0.5em;
    }
    
    [data-testid="stMetricLabel"] {
        font-size: 0.85em;
        color: #718096;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    [data-testid="stMetricValue"] {
        font-size: 2em;
        color: #1a365d;
        font-weight: 600;
    }
    
    [data-testid="stTabs"] [role="tab"] {
        font-weight: 600;
        font-size: 0.95em;
    }
    </style>
    """, unsafe_allow_html=True)
    
    inicializar_componentes()