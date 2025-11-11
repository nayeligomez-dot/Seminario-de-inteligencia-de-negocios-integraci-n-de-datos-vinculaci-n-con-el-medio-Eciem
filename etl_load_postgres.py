import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import logging
import os
from typing import Dict, List, Any
import numpy as np
from datetime import datetime


DB_CONFIG = {
    "host": "localhost",
    "database": "eciem_powerbi",
    "user": "eciem_user",
    "password": "131313",
    "port": "5433"
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)


class DataLoader:
    def __init__(self, db_config: Dict[str, str], data_dir: str = "data"):
        self.db_config = db_config
        self.data_dir = data_dir
        self.conn = None
        self.cur = None
        self.df_comentarios = None
        
    def connect(self):
        """Establecer conexión a PostgreSQL"""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.cur = self.conn.cursor()
            log.info("✅ Conexión a PostgreSQL establecida")
            
        except Exception as e:
            log.error(f"❌ Error conectando a PostgreSQL: {e}")
            raise
    
    def disconnect(self):
        """Cerrar conexión"""
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        log.info("✅ Conexión a PostgreSQL cerrada")
    
    def execute_query(self, query: str, params: tuple = None):
        """Ejecutar query simple"""
        try:
            self.cur.execute(query, params)
            self.conn.commit()
            return True
        except Exception as e:
            self.conn.rollback()
            log.error(f"❌ Error ejecutando query: {e}")
            return False

    def drop_existing_tables(self):
        """Eliminar todas las tablas existentes"""
        log.info("🗑️  Eliminando tablas existentes...")
        
        tables = [
            'hechos', 'practica', 'empresa', 'estudiante', 
            'carrera', 'rubro', 'tiempo', 'evaluacion_empresa'
        ]
        
        for table in tables:
            try:
                self.cur.execute(f"DROP TABLE IF EXISTS public.{table} CASCADE")
                log.info(f"✅ Tabla {table} eliminada")
            except Exception as e:
                log.warning(f"⚠️  No se pudo eliminar tabla {table}: {e}")
        
        self.conn.commit()

    def create_tables(self):
        """Crear todas las tablas del modelo dimensional"""
        log.info("🛠️ Creando tablas en PostgreSQL...")
        
        # Crear tablas en orden para respetar dependencias
        tables_creation = [
            # 1. DIMENSIÓN RUBRO (sin dependencias)
            ("""
                CREATE TABLE public.rubro (
                    id_rubro SERIAL PRIMARY KEY,
                    nombrerubro VARCHAR(255) NOT NULL UNIQUE
                )
            """, "rubro"),
            
            # 2. DIMENSIÓN EMPRESA (depende de rubro) - SECTOR ELIMINADO
            ("""
                CREATE TABLE public.empresa (
                    id_empresa SERIAL PRIMARY KEY,
                    nombre VARCHAR(255) NOT NULL,
                    direccion TEXT,
                    ciudad VARCHAR(100),
                    taranto VARCHAR(100),
                    disposicionrecibir BOOLEAN DEFAULT TRUE,
                    id_rubro INTEGER REFERENCES public.rubro(id_rubro)
                )
            """, "empresa"),
            
            # 3. DIMENSIÓN CARRERA (sin dependencias) - FACULTAD CAMBIADO POR ESCUELA
            ("""
                CREATE TABLE public.carrera (
                    id_carrera SERIAL PRIMARY KEY,
                    nombrecarrera VARCHAR(255) NOT NULL UNIQUE,
                    escuela VARCHAR(255)
                )
            """, "carrera"),
            
            # 4. DIMENSIÓN ESTUDIANTE (sin dependencias)
            ("""
                CREATE TABLE public.estudiante (
                    id_estudiante SERIAL PRIMARY KEY,
                    nombre VARCHAR(255) NOT NULL,
                    apellido VARCHAR(255),
                    correo VARCHAR(255),
                    telefono VARCHAR(50),
                    rut_alumno VARCHAR(20) UNIQUE,
                    matricula VARCHAR(50),
                    anoingreso INTEGER,
                    nivelestudio VARCHAR(100)
                )
            """, "estudiante"),
            
            # 5. DIMENSIÓN TIEMPO (sin dependencias)
            ("""
                CREATE TABLE public.tiempo (
                    id_tiempo SERIAL PRIMARY KEY,
                    anno INTEGER NOT NULL,
                    mes INTEGER NOT NULL,
                    trimestre INTEGER NOT NULL,
                    semestre INTEGER NOT NULL,
                    fechacompleta DATE UNIQUE
                )
            """, "tiempo"),
            
            # 6. DIMENSIÓN EVALUACIÓN EMPRESA (sin dependencias)
            ("""
                CREATE TABLE public.evaluacion_empresa (
                    id_evaluacionempresa SERIAL PRIMARY KEY,
                    comentario TEXT,
                    fecha_evaluacion DATE DEFAULT CURRENT_DATE
                )
            """, "evaluacion_empresa"),
            
            # 7. DIMENSIÓN PRÁCTICA (depende SOLO de carrera - EMPRESA ELIMINADA)
            ("""
                CREATE TABLE public.practica (
                    id_practica SERIAL PRIMARY KEY,
                    modalidad VARCHAR(100),
                    id_carrera INTEGER REFERENCES public.carrera(id_carrera),
                    horaspractica INTEGER,
                    estatus VARCHAR(50),
                    estadoproceso VARCHAR(100),
                    fechainicio DATE,
                    fechafin DATE
                )
            """, "practica"),
            
            # 8. TABLA DE HECHOS (depende de todas las dimensiones INCLUYENDO evaluacion_empresa)
            ("""
                CREATE TABLE public.hechos (
                    id_hechos SERIAL PRIMARY KEY,
                    id_estudiante INTEGER REFERENCES public.estudiante(id_estudiante),
                    id_practica INTEGER REFERENCES public.practica(id_practica),
                    id_empresa INTEGER REFERENCES public.empresa(id_empresa),
                    id_tiempo INTEGER REFERENCES public.tiempo(id_tiempo),
                    id_evaluacionempresa INTEGER REFERENCES public.evaluacion_empresa(id_evaluacionempresa),
                    id_rotacionempresa INTEGER,
                    fecharegistro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """, "hechos")
        ]
        
        for query, table_name in tables_creation:
            success = self.execute_query(query)
            if success:
                log.info(f"✅ Tabla {table_name} creada")
            else:
                log.error(f"❌ Error creando tabla {table_name}")
                raise Exception(f"No se pudo crear la tabla {table_name}")
        
        log.info("🎉 Todas las tablas creadas exitosamente")

    def extraer_comentarios_reales(self):
        """Extraer comentarios reales de las columnas de evaluación"""
        log.info("🔍 Extrayendo comentarios reales de las evaluaciones...")
        
        try:
            file_path = os.path.join(self.data_dir, "alumn_pract_eva_jef.csv")
            if not os.path.exists(file_path):
                log.error(f"❌ Archivo no encontrado: {file_path}")
                return None
                
            df = pd.read_csv(file_path)
            
            # Lista de columnas que pueden contener comentarios
            columnas_comentarios = [
                'r_2_1', 'r_2_2', 'r_2_3', 'r_2_4', 'r_2_5', 'r_2_6',
                'r_2_8', 'r_2_9', 'r_2_10', 'r_2_11', 'r_2_12', 'r_2_13',
                'r_2_14', 'r_2_15'
            ]

            # Valores cortos a excluir
            excluir = ['A', 'B', 'C', 'D', 'Si', 'SI', 'NO', 'No', 'De acuerdo', '1', '2', '3']

            # Función para extraer solo comentarios de texto
            def extraer_comentarios_reales_fila(fila):
                comentarios = []
                for col in columnas_comentarios:
                    if col in fila:
                        valor = fila[col]
                        if pd.notna(valor) and isinstance(valor, str):
                            valor_limpio = valor.strip()
                            # Excluir valores cortos y solo quedarse con texto real
                            if (valor_limpio not in excluir and 
                                len(valor_limpio) > 10 and  # Solo texto con cierta longitud
                                not valor_limpio.isupper() and  # Excluir texto TODO en mayúsculas muy corto
                                not valor_limpio.replace(' ', '').isalpha() and  # Excluir solo letras
                                (',' in valor_limpio or '.' in valor_limpio or ' ' in valor_limpio)):  # Debe tener puntuación o espacios
                                comentarios.append(valor_limpio)
                return ' | '.join(comentarios) if comentarios else "Sin comentarios específicos"

            # Aplicar la función a cada fila
            df['comentarios_evaluacion'] = df.apply(extraer_comentarios_reales_fila, axis=1)

            # Guardar resultado para debug
            output_path = os.path.join(self.data_dir, "alumnos_con_comentarios_reales.csv")
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            log.info(f"✅ Comentarios extraídos y guardados en: {output_path}")

            # Mostrar estadísticas
            comentarios_reales = df[df['comentarios_evaluacion'] != "Sin comentarios específicos"]
            log.info(f"📊 Comentarios reales encontrados: {len(comentarios_reales)} de {len(df)} registros")

            # Mostrar algunos ejemplos
            log.info("--- Ejemplos de comentarios extraídos ---")
            for i, row in comentarios_reales.head(3).iterrows():
                nombre = f"{row.get('nomb_alum', '')} {row.get('apell_alum', '')}".strip()
                comentario = row['comentarios_evaluacion'][:100] + "..." if len(row['comentarios_evaluacion']) > 100 else row['comentarios_evaluacion']
                log.info(f"  {nombre}: {comentario}")

            self.df_comentarios = df
            return df

        except Exception as e:
            log.error(f"❌ Error extrayendo comentarios: {e}")
            return None

    def load_dimension_tables(self):
        """Cargar todas las tablas de dimensión"""
        log.info("📥 Comenzando carga de tablas de dimensión...")
        
        # Extraer comentarios primero si no se ha hecho
        if self.df_comentarios is None:
            self.extraer_comentarios_reales()
        
        # Cargar en orden para respetar dependencias
        self._load_rubro()
        self._load_empresa()
        self._load_carrera()
        self._load_estudiante()
        self._load_tiempo()
        # NO cargamos evaluacion_empresa aquí, se creará en hechos
        self._load_practica()
        
        log.info("✅ Todas las dimensiones cargadas")
    
    def load_fact_table(self):
        """Cargar tabla de hechos principal"""
        log.info("📊 Comenzando carga de tabla de hechos...")
        self._load_hechos()
        log.info("✅ Tabla de hechos cargada")
    
    def _load_rubro(self):
        """Cargar dimensión Rubro desde act_econo_empresa"""
        log.info("Cargando dimensión Rubro...")
        
        try:
            file_path = os.path.join(self.data_dir, "ALUMN_PRACT.csv")
            if not os.path.exists(file_path):
                log.error(f"❌ Archivo no encontrado: {file_path}")
                return
                
            df = pd.read_csv(file_path)
            
            # Extraer rubros únicos de act_econo_empresa
            rubros = set()
            if 'act_econo_empresa' in df.columns:
                unique_rubros = df['act_econo_empresa'].dropna().unique()
                rubros.update([r for r in unique_rubros if str(r).strip()])
            
            # Insertar rubros
            for rubro in rubros:
                self.execute_query(
                    "INSERT INTO public.rubro (nombrerubro) VALUES (%s) ON CONFLICT (nombrerubro) DO NOTHING",
                    (str(rubro).strip(),)
                )
            
            log.info(f"✅ Dimensión Rubro: {len(rubros)} rubros insertados")
            
        except Exception as e:
            log.error(f"❌ Error cargando rubros: {e}")
    
    def _load_empresa(self):
        """Cargar dimensión Empresa - SECTOR ELIMINADO"""
        log.info("Cargando dimensión Empresa...")
        
        try:
            file_path = os.path.join(self.data_dir, "ALUMN_PRACT.csv")
            if not os.path.exists(file_path):
                log.error(f"❌ Archivo no encontrado: {file_path}")
                return
                
            df = pd.read_csv(file_path)
            
            empresas_procesadas = 0
            empresas_unicas = set()
            
            for _, row in df.iterrows():
                if pd.notna(row.get('nomb_empr_prac')):
                    nombre_empresa = str(row['nomb_empr_prac']).strip()
                    
                    # Evitar duplicados en memoria
                    if nombre_empresa in empresas_unicas:
                        continue
                    empresas_unicas.add(nombre_empresa)
                    
                    # Obtener Id_Rubro
                    id_rubro = None
                    if pd.notna(row.get('act_econo_empresa')):
                        self.cur.execute(
                            "SELECT id_rubro FROM public.rubro WHERE nombrerubro = %s", 
                            (str(row['act_econo_empresa']).strip(),)
                        )
                        result = self.cur.fetchone()
                        id_rubro = result[0] if result else None
                    
                    # Insertar empresa (SIN sector)
                    success = self.execute_query("""
                        INSERT INTO public.empresa (nombre, direccion, ciudad, taranto, id_rubro)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        nombre_empresa,
                        str(row['dir_empr_prac']).strip() if pd.notna(row.get('dir_empr_prac')) else None,
                        str(row['ciudad_emp_prac']).strip() if pd.notna(row.get('ciudad_emp_prac')) else None,
                        str(row['nat_empresa']).strip() if pd.notna(row.get('nat_empresa')) else None,
                        id_rubro
                    ))
                    
                    if success:
                        empresas_procesadas += 1
            
            log.info(f"✅ Dimensión Empresa: {empresas_procesadas} empresas procesadas")
            
        except Exception as e:
            log.error(f"❌ Error cargando empresas: {e}")
    
    def _load_carrera(self):
        """Cargar dimensión Carrera - ESCUELA DE CIENCIAS EMPRESARIALES"""
        log.info("Cargando dimensión Carrera...")
        
        try:
            # Crear las 2 carreras oficiales con ESCUELA
            carreras_oficiales = [
                ('INGENIERIA EN INFORMACION Y CONTROL DE GESTION', 'Escuela de Ciencias Empresariales'),
                ('INGENIERIA COMERCIAL', 'Escuela de Ciencias Empresariales')
            ]
            
            for nombre_oficial, escuela in carreras_oficiales:
                self.execute_query("""
                    INSERT INTO public.carrera (nombrecarrera, escuela)
                    VALUES (%s, %s) 
                    ON CONFLICT (nombrecarrera) DO NOTHING
                """, (nombre_oficial, escuela))
            
            log.info(f"✅ Carreras oficiales creadas: {len(carreras_oficiales)}")
            
        except Exception as e:
            log.error(f"❌ Error cargando carreras: {e}")
    
    def _mapear_carrera(self, nombre_carrera):
        """Mapear nombre de carrera a formato oficial"""
        if pd.isna(nombre_carrera):
            return 'INGENIERIA EN INFORMACION Y CONTROL DE GESTION'
            
        carrera_limpia = str(nombre_carrera).strip().upper()
        
        # Mapeo de carreras
        mapeo_carreras = {
            # Ingeniería en Información y Control de Gestión
            'INGENIERIA EN INFORMACION Y CONTROL DE GESTION': 'INGENIERIA EN INFORMACION Y CONTROL DE GESTION',
            'ING. EN INFORMACIÓN Y CONTROL DE GESTIÓN': 'INGENIERIA EN INFORMACION Y CONTROL DE GESTION',
            'INGENIERÍA EN INFORMACIÓN Y CONTROL DE GESTIÓN': 'INGENIERIA EN INFORMACION Y CONTROL DE GESTION',
            'ING. INFORMACION Y CONTROL DE GESTION': 'INGENIERIA EN INFORMACION Y CONTROL DE GESTION',
            'INGENIERIA INFORMACION Y CONTROL GESTION': 'INGENIERIA EN INFORMACION Y CONTROL DE GESTION',
            'IICG': 'INGENIERIA EN INFORMACION Y CONTROL DE GESTION',
            'INGENIERIA EN INFORMACION Y CONTROL DE GESTIÓN': 'INGENIERIA EN INFORMACION Y CONTROL DE GESTION',
            
            # Ingeniería Comercial
            'INGENIERIA COMERCIAL': 'INGENIERIA COMERCIAL',
            'ING. COMERCIAL': 'INGENIERIA COMERCIAL',
            'INGENIERÍA COMERCIAL': 'INGENIERIA COMERCIAL',
            'ING COMERCIAL': 'INGENIERIA COMERCIAL',
            'IC': 'INGENIERIA COMERCIAL'
        }
        
        return mapeo_carreras.get(carrera_limpia, 'INGENIERIA EN INFORMACION Y CONTROL DE GESTION')
    
    def _load_estudiante(self):
        """Cargar dimensión Estudiante"""
        log.info("Cargando dimensión Estudiante...")
        
        try:
            file_path = os.path.join(self.data_dir, "ALUMN_PRACT.csv")
            if not os.path.exists(file_path):
                log.error(f"❌ Archivo no encontrado: {file_path}")
                return
                
            df = pd.read_csv(file_path)
            
            estudiantes_procesados = 0
            for _, row in df.iterrows():
                if pd.notna(row.get('rut_alum')):
                    success = self.execute_query("""
                        INSERT INTO public.estudiante (nombre, apellido, correo, telefono, rut_alumno, 
                                              matricula, anoingreso, nivelestudio)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (rut_alumno) DO NOTHING
                    """, (
                        str(row['nomb_alum']).strip() if pd.notna(row.get('nomb_alum')) else None,
                        str(row['apell_alum']).strip() if pd.notna(row.get('apell_alum')) else None,
                        str(row['email_alum']).strip() if pd.notna(row.get('email_alum')) else None,
                        str(row['cel_alum']).strip() if pd.notna(row.get('cel_alum')) else None,
                        str(row['rut_alum']).strip(),
                        str(row['mat_alum']).strip() if pd.notna(row.get('mat_alum')) else None,
                        self._safe_int(row.get('ano_ingreso_alum')),
                        str(row['niv_estudio_alum']).strip() if pd.notna(row.get('niv_estudio_alum')) else None
                    ))
                    
                    if success:
                        estudiantes_procesados += 1
            
            log.info(f"✅ Dimensión Estudiante: {estudiantes_procesados} estudiantes procesados")
            
        except Exception as e:
            log.error(f"❌ Error cargando estudiantes: {e}")
    
    def _load_tiempo(self):
        """Cargar dimensión Tiempo basado en fechas de prácticas"""
        log.info("Cargando dimensión Tiempo...")
        
        try:
            file_path = os.path.join(self.data_dir, "ALUMN_PRACT.csv")
            if not os.path.exists(file_path):
                log.error(f"❌ Archivo no encontrado: {file_path}")
                return
                
            df = pd.read_csv(file_path)
            
            fechas_unicas = set()
            
            # Procesar fechas de inicio
            if 'fech_ini_prac' in df.columns:
                for fecha_str in df['fech_ini_prac'].dropna():
                    fecha = self._parse_fecha(fecha_str)
                    if fecha:
                        fechas_unicas.add(fecha)
            
            # Procesar fechas de fin
            if 'fech_fin_prac' in df.columns:
                for fecha_str in df['fech_fin_prac'].dropna():
                    fecha = self._parse_fecha(fecha_str)
                    if fecha:
                        fechas_unicas.add(fecha)
            
            # Insertar dimensiones de tiempo
            for fecha in fechas_unicas:
                anno = fecha.year
                mes = fecha.month
                trimestre = (mes - 1) // 3 + 1
                semestre = 1 if mes <= 6 else 2
                
                self.execute_query("""
                    INSERT INTO public.tiempo (anno, mes, trimestre, semestre, fechacompleta)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (fechacompleta) DO NOTHING
                """, (anno, mes, trimestre, semestre, fecha))
            
            log.info(f"✅ Dimensión Tiempo: {len(fechas_unicas)} fechas insertadas")
            
        except Exception as e:
            log.error(f"❌ Error cargando dimensión tiempo: {e}")
    
    def _load_practica(self):
        """Cargar dimensión Práctica SIN conexión a empresa"""
        log.info("Cargando dimensión Práctica...")
        
        try:
            file_path = os.path.join(self.data_dir, "ALUMN_PRACT.csv")
            if not os.path.exists(file_path):
                log.error(f"❌ Archivo no encontrado: {file_path}")
                return
                
            df = pd.read_csv(file_path)
            
            practicas_procesadas = 0
            carreras_mapeadas = 0
            
            for index, row in df.iterrows():
                # Obtener carrera - APLICAR MAPEO
                id_carrera = None
                if pd.notna(row.get('carr_alum')):
                    carrera_mapeada = self._mapear_carrera(row['carr_alum'])
                    
                    # Contar si fue mapeada
                    if str(row['carr_alum']).strip().upper() != carrera_mapeada:
                        carreras_mapeadas += 1
                    
                    self.cur.execute(
                        "SELECT id_carrera FROM public.carrera WHERE nombrecarrera = %s", 
                        (carrera_mapeada,)
                    )
                    result = self.cur.fetchone()
                    id_carrera = result[0] if result else None
                
                # Obtener estatus desde proces_terminado
                estatus = None
                if pd.notna(row.get('proces_terminado')):
                    estatus = str(row['proces_terminado']).strip()
                
                # Calcular horas aproximadas (si hay fechas)
                horas_practica = None
                fecha_inicio = self._parse_fecha(row.get('fech_ini_prac'))
                fecha_fin = self._parse_fecha(row.get('fech_fin_prac'))
                
                if fecha_inicio and fecha_fin:
                    dias = (fecha_fin - fecha_inicio).days
                    if dias > 0:
                        horas_practica = dias * 8
                
                # Insertar práctica (SIN id_empresa e id_evaluacionempresa)
                success = self.execute_query("""
                    INSERT INTO public.practica (modalidad, id_carrera, horaspractica, 
                                        estatus, estadoproceso, fechainicio, fechafin)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    str(row['como_practi_profesional']).strip() if pd.notna(row.get('como_practi_profesional')) else None,
                    id_carrera,
                    horas_practica,
                    estatus,
                    str(row['proces_terminado']).strip() if pd.notna(row.get('proces_terminado')) else None,
                    fecha_inicio,
                    fecha_fin
                ))
                
                if success:
                    practicas_procesadas += 1
            
            log.info(f"✅ Dimensión Práctica: {practicas_procesadas} prácticas insertadas, {carreras_mapeadas} carreras mapeadas")
            
        except Exception as e:
            log.error(f"❌ Error cargando prácticas: {e}")
    
    def _load_hechos(self):
        """Cargar tabla de hechos principal CON evaluacion_empresa - CORREGIDO PARA POWER BI"""
        log.info("Cargando tabla de hechos...")
        
        try:
            file_path = os.path.join(self.data_dir, "ALUMN_PRACT.csv")
            if not os.path.exists(file_path):
                log.error(f"❌ Archivo no encontrado: {file_path}")
                return
                
            df = pd.read_csv(file_path)
            
            hechos_procesados = 0
            evaluaciones_creadas = 0
            
            for index, row in df.iterrows():
                if pd.notna(row.get('rut_alum')):
                    # Obtener IDs de las dimensiones
                    id_estudiante = self._get_id_estudiante_por_rut(str(row['rut_alum']).strip())
                    id_practica = self._get_id_practica_por_indice(index)
                    id_empresa = self._get_id_empresa_por_nombre(str(row['nomb_empr_prac']).strip()) if pd.notna(row.get('nomb_empr_prac')) else None
                    id_tiempo = self._get_id_tiempo_por_fecha(self._parse_fecha(row.get('fech_ini_prac')))
                    
                    # ✅ CORRECCIÓN CRÍTICA: Crear una evaluación ÚNICA para cada registro de hechos
                    comentario = self._obtener_comentario_por_indice(index)
                    
                    # Insertar evaluación única
                    success_eval = self.execute_query("""
                        INSERT INTO public.evaluacion_empresa (comentario) 
                        VALUES (%s) 
                        RETURNING id_evaluacionempresa
                    """, (comentario,))
                    
                    if success_eval:
                        result = self.cur.fetchone()
                        id_evaluacionempresa = result[0] if result else None
                        evaluaciones_creadas += 1
                    else:
                        # Fallback
                        id_evaluacionempresa = index + 1
                
                    if id_estudiante and id_practica and id_evaluacionempresa:
                        success = self.execute_query("""
                            INSERT INTO public.hechos (id_estudiante, id_practica, id_empresa, 
                                            id_tiempo, id_evaluacionempresa, id_rotacionempresa)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (id_estudiante, id_practica, id_empresa, id_tiempo, id_evaluacionempresa, 1))
                        
                        if success:
                            hechos_procesados += 1
                    else:
                        log.warning(f"⚠️  No se pudo insertar hecho para índice {index}")
            
            log.info(f"✅ Tabla Hechos: {hechos_procesados} registros insertados")
            log.info(f"📊 Evaluaciones creadas: {evaluaciones_creadas}")
            
        except Exception as e:
            log.error(f"❌ Error cargando tabla de hechos: {e}")
    
    def _obtener_comentario_por_indice(self, index: int) -> str:
        """Obtener comentario específico para el índice - CORREGIDO"""
        try:
            if self.df_comentarios is not None and index < len(self.df_comentarios):
                comentario = self.df_comentarios.iloc[index]['comentarios_evaluacion']
                if comentario and comentario != "Sin comentarios específicos":
                    return f"Evaluación {index+1}: {comentario[:200]}"
            return f"Evaluación profesional {index+1} - Práctica estudiantil"
        except Exception as e:
            log.warning(f"⚠️  Error obteniendo comentario para índice {index}: {e}")
            return f"Evaluación estándar {index+1}"
    
    # =====================================================
    # MÉTODOS AUXILIARES
    # =====================================================
    def _safe_int(self, value):
        """Convertir a int de forma segura"""
        try:
            return int(float(value)) if pd.notna(value) else None
        except:
            return None
    
    def _safe_float(self, value):
        """Convertir a float de forma segura"""
        try:
            return float(value) if pd.notna(value) else None
        except:
            return None
    
    def _parse_fecha(self, fecha_str):
        """Parsear fecha de forma segura"""
        if pd.isna(fecha_str) or fecha_str in ['', '0000-00-00', '0000-00-00 00:00:00']:
            return None
        try:
            return pd.to_datetime(fecha_str).date()
        except:
            return None
    
    def _get_id_estudiante_por_rut(self, rut: str) -> int:
        """Obtener ID de estudiante por RUT"""
        self.cur.execute("SELECT id_estudiante FROM public.estudiante WHERE rut_alumno = %s", (rut,))
        result = self.cur.fetchone()
        return result[0] if result else None
    
    def _get_id_empresa_por_nombre(self, nombre: str) -> int:
        """Obtener ID de empresa por nombre"""
        self.cur.execute("SELECT id_empresa FROM public.empresa WHERE nombre = %s", (nombre,))
        result = self.cur.fetchone()
        return result[0] if result else None
    
    def _get_id_tiempo_por_fecha(self, fecha) -> int:
        """Obtener ID de tiempo por fecha"""
        if not fecha:
            return None
        self.cur.execute("SELECT id_tiempo FROM public.tiempo WHERE fechacompleta = %s", (fecha,))
        result = self.cur.fetchone()
        return result[0] if result else None
    
    def _get_id_practica_por_indice(self, index: int) -> int:
        """Obtener ID de práctica por índice (correlativo)"""
        self.cur.execute("SELECT id_practica FROM public.practica ORDER BY id_practica LIMIT 1 OFFSET %s", (index,))
        result = self.cur.fetchone()
        return result[0] if result else None


def main():
    """Función principal para ejecutar la carga de datos completa"""
    
    loader = DataLoader(DB_CONFIG)
    
    try:
        # Conectar a la base de datos
        loader.connect()
        
        # ELIMINAR TABLAS EXISTENTES PRIMERO
        loader.drop_existing_tables()
        
        # Crear tablas (con la estructura corregida)
        loader.create_tables()
        
        # Extraer comentarios reales antes de cargar dimensiones
        loader.extraer_comentarios_reales()
        
        # Cargar dimensiones primero
        loader.load_dimension_tables()
        
        # Cargar tabla de hechos
        loader.load_fact_table()
        
        log.info("🎉 ¡CARGA COMPLETADA EXITOSAMENTE!")
        
    except Exception as e:
        log.error(f"❌ Error en el proceso de carga: {e}")
    finally:
        loader.disconnect()

if __name__ == "__main__":
    main()