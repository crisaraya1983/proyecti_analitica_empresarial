## Tabla de Contenidos

- [Descripción General](#descripción-general)
- [Requisitos Previos](#requisitos-previos)
- [Instalación](#instalación)
  - [Alternativa 1: Con Base de Datos Respaldada (Recomendado)](#alternativa-1-con-base-de-datos-respaldada-recomendado)
  - [Alternativa 2: Crear Base de Datos Desde Cero](#alternativa-2-crear-base-de-datos-desde-cero)
- [Configuración de Credenciales](#configuración-de-credenciales)
- [Ejecución](#ejecución)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Características Principales](#características-principales)

---

## ✅ Requisitos Previos

### Software Requerido

- **SQL Server 2019+**: Para las bases de datos OLTP y DW
- **Python 3.9+**: Para la aplicación Streamlit

### Archivos de Respaldo

Debes tener disponibles los siguientes archivos:

```
Ecommerce_OLTP_backup.bak        # Respaldo de base de datos OLTP
Ecommerce_DW_backup.bak          # Respaldo de base de datos DW (opcional)
```

---

## Instalación

### 🔵 **Alternativa 1: Con Base de Datos Respaldada (RECOMENDADO)**

#### Paso 1: Restaurar Base de Datos OLTP

1. **Abre SQL Server Management Studio (SSMS)**

2. **Conéctate al servidor SQL Server**

3. **Restaurar la base de datos OLTP**

4. **Verifica la restauración**

#### Paso 2: Elegir entre Opción A o B

##### 📦 Opción A: Restaurar Base de Datos DW desde Respaldo (MÁS RÁPIDO)

**Ventajas**:
- No requiere ejecutar ETL
- Base de datos lista para usar inmediatamente
- Todos los datos ya cargados

##### 🔄 Opción B: Crear Base de Datos DW Desde Cero + Ejecutar ETL

**Paso 2.1**: Crear la estructura de base de datos
```sql
-- Abre el archivo: Scripts_SQL_Server/2_Crear_Base_Datos_DW.sql
-- Ejecuta el script completo en SSMS
```

#### Paso 3: Ejecutar ETL (SOLO SI SE USO OPCION B)

---

## 🔐 Configuración de Credenciales

### Opción 1: Variables de Entorno Personales

Modificar archivo `.streamlit/secrets.toml` en el directorio raíz del proyecto:

Del archivo existente solo modificar el SERVER por el nombre de tu instancia MSSQL (En caso de acceso por Windows)

[sqlserver]
server = "cambiar_esto"
database_oltp = "Ecommerce_OLTP"
database_dw = "Ecommerce_DW"
driver = "ODBC Driver 17 for SQL Server"
trusted_connection = "yes"

En caso de Acceso por Credenciales usar esto

[sqlserver]
server = "localhost"           # O tu servidor SQL Server
database = "Ecommerce_OLTP"
user = "tu_usuario"            # Ej: sa
password = "tu_contraseña"
driver = "{ODBC Driver 17 for SQL Server}"

### Opción 2: Variables de Entorno del Sistema

```bash
# Windows (PowerShell)
$env:OLTP_SERVER = "localhost"
$env:OLTP_DATABASE = "Ecommerce_OLTP"
$env:OLTP_USER = "sa"
$env:OLTP_PASSWORD = "tu_contraseña"

$env:DW_SERVER = "localhost"
$env:DW_DATABASE = "Ecommerce_DW"
$env:DW_USER = "sa"
$env:DW_PASSWORD = "tu_contraseña"

$env:ANTHROPIC_API_KEY = "tu_clave_api"
```

---

## 🎯 Ejecución

### Paso 1: Instalar Dependencias Python

```bash
# Navega a la carpeta del proyecto
cd streamlit_app

# Crea un entorno virtual
python -m venv venv

# Activa el entorno virtual
# En Windows:
.\venv\Scripts\activate

# Instala las dependencias
pip install -r requirements.txt
```

### Paso 2: Ejecutar ETL (Si usas Alternativa 2 Opción B)

```bash
# Desde la carpeta del proyecto
cd streamlit_app

# Ejecuta el ETL
streamlit run Home.py
```

1. Abre la navegación lateral
2. Ve a **"ETL - Carga de Data Warehouse"**
3. Haz click en **"INICIAR PROCESO ETL"**
4. Espera a que se complete (10-15 minutos dependiendo del volumen de datos)

**Monitoreo del ETL**:
- Verás los logs en tiempo real
- Indicador de progreso
- Resumen de registros cargados
- Estado final (exitoso o con errores)

### Paso 3: Ejecutar Aplicación Streamlit (Si se restauro la DW)

```bash
# Desde la carpeta streamlit_app
streamlit run Home.py
```

**Salida esperada**:
```
  You can now view your Streamlit app in your browser.

  Local URL: http://localhost:8501
  Network URL: http://192.168.1.X:8501
```

4. Abre tu navegador en `http://localhost:8501`

---