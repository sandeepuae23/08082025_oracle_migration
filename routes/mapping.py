from flask import Blueprint, request, jsonify
from models import MappingConfiguration, OracleConnection, ElasticsearchConnection
from services.mapping_service import MappingService
from app import db
import logging

mapping_bp = Blueprint('mapping', __name__)
logger = logging.getLogger(__name__)

@mapping_bp.route('/configurations', methods=['GET'])
def get_configurations():
    """Get all mapping configurations"""
    try:
        configs = MappingConfiguration.query.filter_by(is_active=True).all()
        return jsonify([{ 
            'id': config.id,
            'name': config.name,
            'oracle_connection': config.oracle_connection.name,
            'elasticsearch_connection': config.elasticsearch_connection.name,
            'elasticsearch_index': config.elasticsearch_index,
            'incremental_column': config.incremental_column,
            'last_sync_time': config.last_sync_time.isoformat() if config.last_sync_time else None,
            'schedule_interval': config.schedule_interval,
            'created_at': config.created_at.isoformat(),
            'updated_at': config.updated_at.isoformat()
        } for config in configs])
    except Exception as e:
        logger.error(f"Error fetching mapping configurations: {str(e)}")
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/configurations', methods=['POST'])
def create_configuration():
    """Create a new mapping configuration"""
    try:
        data = request.json
        config = MappingConfiguration(
            name=data['name'],
            oracle_connection_id=data['oracle_connection_id'],
            elasticsearch_connection_id=data['elasticsearch_connection_id'],
            oracle_query=data['oracle_query'],
            elasticsearch_index=data['elasticsearch_index'],
            incremental_column=data.get('incremental_column'),
            schedule_interval=data.get('schedule_interval')
        )
        config.set_field_mappings(data.get('field_mappings', []))
        config.set_transformation_rules(data.get('transformation_rules', []))
        
        db.session.add(config)
        db.session.commit()
        
        return jsonify({'id': config.id, 'message': 'Configuration created successfully'})
    except Exception as e:
        logger.error(f"Error creating mapping configuration: {str(e)}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/configurations/<int:config_id>', methods=['GET'])
def get_configuration(config_id):
    """Get a specific mapping configuration"""
    try:
        config = MappingConfiguration.query.get_or_404(config_id)
        return jsonify({
            'id': config.id,
            'name': config.name,
            'oracle_connection_id': config.oracle_connection_id,
            'elasticsearch_connection_id': config.elasticsearch_connection_id,
            'oracle_query': config.oracle_query,
            'elasticsearch_index': config.elasticsearch_index,
            'incremental_column': config.incremental_column,
            'last_sync_time': config.last_sync_time.isoformat() if config.last_sync_time else None,
            'schedule_interval': config.schedule_interval,
            'field_mappings': config.get_field_mappings(),
            'transformation_rules': config.get_transformation_rules(),
            'created_at': config.created_at.isoformat(),
            'updated_at': config.updated_at.isoformat()
        })
    except Exception as e:
        logger.error(f"Error fetching mapping configuration: {str(e)}")
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/configurations/<int:config_id>', methods=['PUT'])
def update_configuration(config_id):
    """Update a mapping configuration"""
    try:
        config = MappingConfiguration.query.get_or_404(config_id)
        data = request.json
        
        config.name = data.get('name', config.name)
        config.oracle_query = data.get('oracle_query', config.oracle_query)
        config.elasticsearch_index = data.get('elasticsearch_index', config.elasticsearch_index)
        if 'incremental_column' in data:
            config.incremental_column = data['incremental_column']
        if 'schedule_interval' in data:
            config.schedule_interval = data['schedule_interval']

        if 'field_mappings' in data:
            config.set_field_mappings(data['field_mappings'])
        if 'transformation_rules' in data:
            config.set_transformation_rules(data['transformation_rules'])
        
        db.session.commit()
        return jsonify({'message': 'Configuration updated successfully'})
    except Exception as e:
        logger.error(f"Error updating mapping configuration: {str(e)}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/auto-suggest', methods=['POST'])
def auto_suggest_mapping():
    """Generate automatic mapping suggestions"""
    try:
        data = request.json
        oracle_connection_id = data['oracle_connection_id']
        elasticsearch_connection_id = data['elasticsearch_connection_id']
        oracle_query = data['oracle_query']
        elasticsearch_index = data['elasticsearch_index']
        
        oracle_conn = OracleConnection.query.get_or_404(oracle_connection_id)
        es_conn = ElasticsearchConnection.query.get_or_404(elasticsearch_connection_id)
        
        mapping_service = MappingService(oracle_conn, es_conn)
        suggestions = mapping_service.generate_auto_mapping(oracle_query, elasticsearch_index)
        
        return jsonify(suggestions)
    except Exception as e:
        logger.error(f"Error generating auto mapping: {str(e)}")
        return jsonify({'error': str(e)}), 500


@mapping_bp.route('/field-analysis', methods=['POST'])
def field_analysis():
    """Analyze Oracle query fields and suggest Elasticsearch mappings"""
    try:
        data = request.json
        oracle_connection_id = data['oracle_connection_id']
        elasticsearch_connection_id = data['elasticsearch_connection_id']
        oracle_query = data['oracle_query']
        elasticsearch_index = data['elasticsearch_index']

        oracle_conn = OracleConnection.query.get_or_404(oracle_connection_id)
        es_conn = ElasticsearchConnection.query.get_or_404(elasticsearch_connection_id)

        mapping_service = MappingService(oracle_conn, es_conn)
        analysis = mapping_service.generate_auto_mapping(oracle_query, elasticsearch_index)

        return jsonify({
            'suggested_mappings': analysis['suggested_mappings'],
            'transformation_rules': analysis['transformation_rules']
        })
    except Exception as e:
        logger.error(f"Error analyzing fields: {str(e)}")
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/validate', methods=['POST'])
def validate_mapping():
    """Validate field mappings and type compatibility"""
    try:
        data = request.json
        oracle_connection_id = data['oracle_connection_id']
        elasticsearch_connection_id = data['elasticsearch_connection_id']
        field_mappings = data['field_mappings']
        
        oracle_conn = OracleConnection.query.get_or_404(oracle_connection_id)
        es_conn = ElasticsearchConnection.query.get_or_404(elasticsearch_connection_id)
        
        mapping_service = MappingService(oracle_conn, es_conn)
        validation_result = mapping_service.validate_mappings(field_mappings)
        
        return jsonify(validation_result)
    except Exception as e:
        logger.error(f"Error validating mapping: {str(e)}")
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/export/<int:config_id>', methods=['GET'])
def export_configuration(config_id):
    """Export mapping configuration as JSON"""
    try:
        config = MappingConfiguration.query.get_or_404(config_id)
        export_data = {
            'name': config.name,
            'oracle_query': config.oracle_query,
            'elasticsearch_index': config.elasticsearch_index,
            'field_mappings': config.get_field_mappings(),
            'transformation_rules': config.get_transformation_rules(),
            'exported_at': config.updated_at.isoformat()
        }
        return jsonify(export_data)
    except Exception as e:
        logger.error(f"Error exporting configuration: {str(e)}")
        return jsonify({'error': str(e)}), 500

@mapping_bp.route('/import', methods=['POST'])
def import_configuration():
    """Import mapping configuration from JSON"""
    try:
        data = request.json
        
        config = MappingConfiguration(
            name=data['name'],
            oracle_connection_id=data['oracle_connection_id'],
            elasticsearch_connection_id=data['elasticsearch_connection_id'],
            oracle_query=data['oracle_query'],
            elasticsearch_index=data['elasticsearch_index']
        )
        config.set_field_mappings(data.get('field_mappings', []))
        config.set_transformation_rules(data.get('transformation_rules', []))
        
        db.session.add(config)
        db.session.commit()
        
        return jsonify({'id': config.id, 'message': 'Configuration imported successfully'})
    except Exception as e:
        logger.error(f"Error importing configuration: {str(e)}")
        db.session.rollback()
        return jsonify({'error': str(e)}), 500
