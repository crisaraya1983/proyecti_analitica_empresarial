"""
Script de prueba para verificar que todas las funciones de KPICalculator funcionan
"""
import sys
import os

# Agregar rutas
streamlit_path = os.path.join(os.path.dirname(__file__), 'streamlit_app')
sys.path.insert(0, os.path.join(streamlit_path, 'modulos'))
sys.path.insert(0, os.path.join(streamlit_path, 'utils'))

print('='*80)
print('PRUEBA DE FUNCIONES DE KPICalculator')
print('='*80)

from kpis_calculator import KPICalculator
from db_connection import DatabaseConnection

print('\n1. Conectando a base de datos...')
engine = DatabaseConnection.get_dw_engine(use_secrets=True)
print('   ✓ Conectado')

print('\n2. Creando KPICalculator...')
kpi_calc = KPICalculator(engine)
print('   ✓ Creado')

print('\n3. Probando calcular_kpis_principales_2025()...')
try:
    kpis = kpi_calc.calcular_kpis_principales_2025(mes_hasta=10)
    print(f'   ✓ OK - Ventas 2025: ₡{kpis["ventas_totales_2025"]:,.0f}')
    print(f'   ✓ OK - Margen 2025: {kpis["margen_porcentaje_2025"]:.2f}%')
except Exception as e:
    print(f'   ✗ ERROR: {e}')

print('\n4. Probando calcular_funnel_comportamiento_web()...')
try:
    df = kpi_calc.calcular_funnel_comportamiento_web()
    print(f'   ✓ OK - Total sesiones: {df.iloc[0]["cantidad"]:,}')
    print(f'   ✓ OK - Conversión: {df.iloc[5]["cantidad"]:,}')
except Exception as e:
    print(f'   ✗ ERROR: {e}')

print('\n5. Probando calcular_metricas_comportamiento_web()...')
try:
    metricas = kpi_calc.calcular_metricas_comportamiento_web()
    print(f'   ✓ OK - Tasa conversión: {metricas["tasa_conversion"]}%')
    print(f'   ✓ OK - Usuarios únicos: {metricas["usuarios_unicos"]:,}')
except Exception as e:
    print(f'   ✗ ERROR: {e}')

print('\n6. Verificando métodos disponibles...')
metodos_importantes = [
    'calcular_kpis_principales_2025',
    'calcular_funnel_comportamiento_web',
    'calcular_metricas_comportamiento_web',
    'calcular_ventas_totales',
    'calcular_margen_ganancia'
]

for metodo in metodos_importantes:
    if hasattr(kpi_calc, metodo):
        print(f'   ✓ {metodo}')
    else:
        print(f'   ✗ {metodo} NO ENCONTRADO')

print('\n' + '='*80)
print('PRUEBA COMPLETADA')
print('='*80)
