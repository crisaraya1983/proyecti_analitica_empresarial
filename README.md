## ✅ Requisitos Previos

### Software Requerido

- **SQL Server 2019+**: Para las bases de datos OLTP y DW
- **Python 3.9+**: Para la aplicación Streamlit

### Archivos de Respaldo

Debes tener disponibles los siguientes archivos:

```
Ecommerce_OLTP_backup.bak        # Respaldo de base de datos OLTP
Ecommerce_DW_backup.bak          # Respaldo de base de datos DW
```

---

## Instalación

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

1. **Abre SQL Server Management Studio (SSMS)**

2. **Conéctate al servidor SQL Server**

3. **Restaurar la base de datos OLTP**

4. **Verifica la restauración**

##### 🔄 Opción B: Crear Base de Datos DW Desde Cero + Ejecutar ETL

**Paso 2.1**: Crear la estructura de base de datos
```sql
-- Abre el archivo: Scripts_SQL_Server/2_Crear_Base_Datos_DW.sql
-- Ubicacion: \proyecti_analitica_empresarial\Scripts_SQL_Server/2_Crear_Base_Datos_DW.sql
-- Ejecuta el script completo en SSMS
```
---

## 🔐 Configuración de Aplicacion Streamlit

### Paso 1: Variables de Entorno Personales

Modificar archivo `.streamlit/secrets.toml` en el directorio raíz del proyecto:
Ubicacion: \proyecti_analitica_empresarial\streamlit_app\.streamlit\secrets.toml

Del archivo existente solo modificar el SERVER por el nombre de tu instancia MSSQL (En caso de acceso por credenciales de Windows)

Ejemplos: 
server = "PabloG5\\MSSQLSERVERMULTI,1433"  -- Nombre de usuario, servidor y puerto
server = "CRISTIANDELL"  --nombre de servidor
server = "localhost"   --- usando localhost

Modificar Seccion de configuracion [sqlserver] Solamente

[sqlserver]
server = "CAMBIAR_ESTO_SOLAMENTE"
database_oltp = "Ecommerce_OLTP"
database_dw = "Ecommerce_DW"
driver = "ODBC Driver 17 for SQL Server"
trusted_connection = "yes"

En caso de Acceso por Credenciales usar esto:
[sqlserver]
server = "localhost"           # O tu servidor SQL Server
database = "Ecommerce_OLTP"
user = "tu_usuario"            # Ej: sa
password = "tu_contraseña"
driver = "{ODBC Driver 17 for SQL Server}"

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

### Paso 2: Abrir Streamlit y Ejecutar ETL (Si usas Opción B para crear Data Warehouse o Bien para probar funcionalidad)

```bash
# Desde la carpeta del proyecto
cd streamlit_app

# Ejecuta el ETL
streamlit run Home.py
```
Nota: si los datos aun no estan cargados es posible ver errores pero igual navegar a Pagina ETL
Adicionalmente probar la conexion son SQL Server y asegurarse esta funcionando, Se veran errores de conexion en caso de no estar bien configurado el Secrets. 

1. Abre la navegación lateral (Parte Arriba Izquierda de la pantalla - Esta Oculto)
2. Ve a **"ETL - Carga de Data Warehouse"**
3. Haz click en **"INICIAR PROCESO ETL"**
4. Espera a que se complete (10-15 minutos)

**Monitoreo del ETL**:
- Verás los logs en tiempo real
- Indicador de progreso
- Resumen de registros cargados
- Estado final (exitoso o con errores)
- Ignorar mensajes de Warning

### Paso 3: Ejecutar Aplicación Streamlit (Si se restauro la DW desde Backup)

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

4. Abre tu navegador en `http://localhost:8501` en caso de que no se abra automaticamente