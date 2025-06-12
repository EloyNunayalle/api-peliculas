import boto3
import uuid
import os
import json

def lambda_handler(event, context):
    try:
        # Entrada (json)
        print(json.dumps({
            "tipo": "INFO",
            "log_datos": {
                "evento_recibido": event
            }
        }))

        tenant_id = event['body']['tenant_id']
        pelicula_datos = event['body']['pelicula_datos']
        nombre_tabla = os.environ["TABLE_NAME"]

        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)

        # Salida (json)
        print(json.dumps({
            "tipo": "INFO",
            "log_datos": {
                "pelicula_insertada": pelicula,
                "resultado_dynamodb": response.get("ResponseMetadata", {})
            }
        }))

        return {
            'statusCode': 200,
            'pelicula': pelicula,
            'response': response
        }
    
    
    except Exception as e:
        
        import traceback
        # Log de error
        print(json.dumps({
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": "Error en la creación de la película",
                "error": str(e),
                "evento_original": event,
                "traceback": traceback.format_exc()
            }
        }))
        return {
            'statusCode': 500,
            'error': str(e)
        }
