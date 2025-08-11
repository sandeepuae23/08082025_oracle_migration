import oracledb
import logging
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML

logger = logging.getLogger(__name__)

class OracleService:
    def __init__(self, connection_config):
        self.config = connection_config
        self.connection = None
    
    def get_connection(self):
        """Get Oracle database connection"""
        if not self.connection:
            try:
                dsn = oracledb.makedsn(
                    self.config.host, 
                    self.config.port, 
                    service_name=self.config.service_name
                )
                self.connection = oracledb.connect(
                    user=self.config.username,
                    password=self.config.password,
                    dsn=dsn
                )
            except Exception as e:
                logger.error(f"Failed to connect to Oracle: {str(e)}")
                raise
        return self.connection
    
    def test_connection(self):
        """Test Oracle database connection"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"Oracle connection test failed: {str(e)}")
            return False
    
    def get_tables(self):
        """Get all tables from Oracle database"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT table_name, num_rows, last_analyzed
                FROM user_tables 
                ORDER BY table_name
            """)
            
            tables = []
            for row in cursor.fetchall():
                tables.append({
                    'table_name': row[0],
                    'num_rows': row[1],
                    'last_analyzed': row[2].isoformat() if row[2] else None
                })
            
            cursor.close()
            return tables
        except Exception as e:
            logger.error(f"Error fetching tables: {str(e)}")
            raise
    
    def get_table_columns(self, table_name):
        """Get columns for a specific table"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT column_name, data_type, data_length, data_precision, 
                       data_scale, nullable, data_default
                FROM user_tab_columns 
                WHERE table_name = :table_name
                ORDER BY column_id
            """, {'table_name': table_name.upper()})
            
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    'column_name': row[0],
                    'data_type': row[1],
                    'data_length': row[2],
                    'data_precision': row[3],
                    'data_scale': row[4],
                    'nullable': row[5] == 'Y',
                    'data_default': row[6],
                    'elasticsearch_type': self._map_oracle_to_es_type(row[1])
                })
            
            cursor.close()
            return columns
        except Exception as e:
            logger.error(f"Error fetching table columns: {str(e)}")
            raise
    
    def analyze_query(self, query):
        """Analyze SQL query and extract column information"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Use DESCRIBE to get column information without executing the full query
            describe_query = f"SELECT * FROM ({query}) WHERE ROWNUM = 0"
            cursor.execute(describe_query)
            
            columns = []
            for desc in cursor.description:
                column_name = desc[0]
                oracle_type = self._get_oracle_type_name(desc[1])
                
                columns.append({
                    'field': column_name.lower(),
                    'oracle_type': oracle_type,
                    'elasticsearch_type': self._map_oracle_to_es_type(oracle_type),
                    'source': self._extract_source_from_query(query, column_name)
                })
            
            cursor.close()
            
            # Parse query for additional information
            joins = self._extract_joins_from_query(query)
            
            return {
                'columns': columns,
                'joins': joins,
                'query_type': 'SELECT'
            }
        except Exception as e:
            logger.error(f"Error analyzing query: {str(e)}")
            raise
    
    def execute_query(self, query, limit=10):
        """Execute SQL query and return sample results"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Add ROWNUM limit if not present
            if 'ROWNUM' not in query.upper() and 'LIMIT' not in query.upper():
                limited_query = f"SELECT * FROM ({query}) WHERE ROWNUM <= {limit}"
            else:
                limited_query = query
            
            cursor.execute(limited_query)
            
            # Get column names
            column_names = [desc[0] for desc in cursor.description]
            
            # Fetch results
            rows = cursor.fetchall()
            results = []
            for row in rows:
                row_dict = {}
                for i, value in enumerate(row):
                    # Convert Oracle types to JSON-serializable types
                    if hasattr(value, 'isoformat'):  # Date/Datetime
                        row_dict[column_names[i]] = value.isoformat()
                    elif isinstance(value, (int, float, str)) or value is None:
                        row_dict[column_names[i]] = value
                    else:
                        row_dict[column_names[i]] = str(value)
                results.append(row_dict)
            
            cursor.close()
            
            return {
                'columns': column_names,
                'rows': results,
                'total_rows': len(results)
            }
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
    
    def _map_oracle_to_es_type(self, oracle_type):
        """Map Oracle data types to Elasticsearch types"""
        type_mapping = {
            'NUMBER': 'long',
            'FLOAT': 'float',
            'BINARY_FLOAT': 'float',
            'BINARY_DOUBLE': 'double',
            'VARCHAR2': 'text',
            'CHAR': 'keyword',
            'NVARCHAR2': 'text',
            'NCHAR': 'keyword',
            'CLOB': 'text',
            'NCLOB': 'text',
            'DATE': 'date',
            'TIMESTAMP': 'date',
            'TIMESTAMP WITH TIME ZONE': 'date',
            'TIMESTAMP WITH LOCAL TIME ZONE': 'date',
            'BLOB': 'binary',
            'RAW': 'binary',
            'LONG RAW': 'binary'
        }
        
        # Handle NUMBER with precision/scale
        if oracle_type.startswith('NUMBER'):
            if ',' in oracle_type:  # Has decimal places
                return 'double'
            else:
                return 'long'
        
        return type_mapping.get(oracle_type, 'keyword')
    
    def _get_oracle_type_name(self, type_code):
        """Convert Oracle type code to type name"""
        type_codes = {
            oracledb.DB_TYPE_VARCHAR: 'VARCHAR2',
            oracledb.DB_TYPE_CHAR: 'CHAR',
            oracledb.DB_TYPE_NUMBER: 'NUMBER',
            oracledb.DB_TYPE_DATE: 'DATE',
            oracledb.DB_TYPE_TIMESTAMP: 'TIMESTAMP',
            oracledb.DB_TYPE_CLOB: 'CLOB',
            oracledb.DB_TYPE_BLOB: 'BLOB',
            oracledb.DB_TYPE_BINARY_FLOAT: 'BINARY_FLOAT',
            oracledb.DB_TYPE_BINARY_DOUBLE: 'BINARY_DOUBLE'
        }
        return type_codes.get(type_code, 'UNKNOWN')
    
    def _extract_source_from_query(self, query, column_name):
        """Extract source table and column from query"""
        # This is a simplified implementation
        # In a production system, you would want a more sophisticated SQL parser
        try:
            parsed = sqlparse.parse(query)[0]
            # Basic extraction - would need more sophisticated parsing for complex queries
            return f"query.{column_name}"
        except:
            return f"query.{column_name}"
    
    def _extract_joins_from_query(self, query):
        """Extract JOIN information from query"""
        joins = []
        try:
            # Simple regex-based extraction for demonstration
            # In production, use a proper SQL parser
            query_upper = query.upper()
            if 'JOIN' in query_upper:
                # This is a simplified extraction
                # Would need more sophisticated parsing for production
                joins.append({'type': 'INNER', 'condition': 'Detected in query'})
        except:
            pass
        return joins
    
    def close_connection(self):
        """Close Oracle connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
