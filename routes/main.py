from flask import Blueprint, render_template, request, jsonify
from models import OracleConnection, ElasticsearchConnection, MappingConfiguration
from app import db

main_bp = Blueprint('main', __name__)

@main_bp.route('/')
def index():
    """Main dashboard showing overview of connections and mappings"""
    oracle_connections = OracleConnection.query.filter_by(is_active=True).count()
    es_connections = ElasticsearchConnection.query.filter_by(is_active=True).count()
    mappings = MappingConfiguration.query.filter_by(is_active=True).count()
    
    return render_template('index.html', 
                         oracle_connections=oracle_connections,
                         es_connections=es_connections,
                         mappings=mappings)

@main_bp.route('/oracle-explorer')
def oracle_explorer():
    """Oracle database exploration interface"""
    connections = OracleConnection.query.filter_by(is_active=True).all()
    return render_template('oracle_explorer.html', connections=connections)

@main_bp.route('/elasticsearch-explorer')
def elasticsearch_explorer():
    """Elasticsearch cluster exploration interface"""
    connections = ElasticsearchConnection.query.filter_by(is_active=True).all()
    return render_template('elasticsearch_explorer.html', connections=connections)

@main_bp.route('/mapping-interface')
def mapping_interface():
    """Main mapping interface for creating field mappings"""
    oracle_connections = OracleConnection.query.filter_by(is_active=True).all()
    es_connections = ElasticsearchConnection.query.filter_by(is_active=True).all()
    mappings = MappingConfiguration.query.filter_by(is_active=True).all()
    
    return render_template('mapping_interface.html', 
                         oracle_connections=oracle_connections,
                         es_connections=es_connections,
                         mappings=mappings)

@main_bp.route('/migration-status')
def migration_status():
    """Migration status and job monitoring interface"""
    return render_template('migration_status.html')


@main_bp.route('/demo')
def demo():
    """Interactive demo showcasing migration features"""
    return render_template('demo.html')
