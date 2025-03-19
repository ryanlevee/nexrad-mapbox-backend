import io
import json
import os

import boto3
from dotenv import load_dotenv
from flask import Flask, abort, jsonify, request, send_file
from flask_cors import CORS

load_dotenv()

app = Flask(__name__)
CORS(app, origins="http://localhost:3000")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

BUCKET_NAME = "nexrad-mapbox"
BUCKET_PATH_PLOTS_PREFIX = "plots_level"
BUCKET_FLAG_PATH = "flags"
BUCKET_CODE_PATH = "codes"


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Max-Age"] = "86400"
    return response


@app.route("/*", methods=["OPTIONS"])
def handle_options():
    return "", 204

@app.route("/code/", methods=["GET"])
def handle_code_get():
    object_key = f"{BUCKET_CODE_PATH}/options.json"

    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=object_key)
        file_content_bytes = response["Body"].read()
        file_content = json.loads(file_content_bytes.decode("utf-8"))
        return jsonify(file_content), 200
    except s3_client.exceptions.NoSuchKey:
        abort(404)
    except Exception as e:
        print(f"Error accessing S3: {e}")
        abort(500)


@app.route("/flag/", methods=["GET"])
def handle_flag_get():
    object_key = f"{BUCKET_FLAG_PATH}/update_flags.json"

    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=object_key)
        file_content_bytes = response["Body"].read()
        file_content = json.loads(file_content_bytes.decode("utf-8"))
        return jsonify(file_content), 200
    except s3_client.exceptions.NoSuchKey:
        abort(404)
    except Exception as e:
        print(f"Error accessing S3: {e}")
        abort(500)


@app.route("/flag/", methods=["POST"])
def handle_flag_post():
    try:
        body = request.get_json()

        if not body:
            return jsonify({"error": "Request body is empty or not valid JSON"}), 400

        object_key = f"{BUCKET_FLAG_PATH}/update_flags.json"

        if update_json_in_s3(object_key, body):
            return (
                jsonify(
                    {"updated": True, "message": f"'{object_key}' updated in S3!"}
                ),
                200,
            )
        else:
            return jsonify({"error": "Failed to update file in S3"}), 500

    except Exception as e:
        print(f"Error processing update request: {e}")
        return jsonify({"error": "Internal Server Error"}), 500


@app.route("/list/<path:level>/<path:product>/", methods=["GET"])
def handle_list_get(level, product):
    object_key = f"lists/nexrad_level{level}_{product}_files.json"

    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=object_key)
        file_content_bytes = response["Body"].read()
        file_content = json.loads(file_content_bytes.decode("utf-8"))
        return jsonify(file_content), 200
    except s3_client.exceptions.NoSuchKey:
        abort(404)
    except Exception as e:
        print(f"Error accessing S3: {e}")
        abort(500)


@app.route("/list-all/", methods=["GET"])
def handle_list_all_get():
    data = {}

    levels_and_products = [
        {"product": "reflectivity", "level": 2},
        {"product": "hydrometeor", "level": 3},
        {"product": "precipitation", "level": 3},
    ]

    object_key = "lists/nexrad_level{level}_{product}_files.json"

    try:
        for item in levels_and_products:
            product = item["product"]
            level = item["level"]
            formatted_key = object_key.format(level=level, product=product)
            print(formatted_key)
            response = s3_client.get_object(Bucket=BUCKET_NAME, Key=formatted_key)
            file_content_bytes = response["Body"].read()
            data[product] = json.loads(file_content_bytes.decode("utf-8"))

        return jsonify(data), 200

    except s3_client.exceptions.NoSuchKey:
        abort(404)
    except Exception as e:
        print(f"Error accessing S3: {e}")
        abort(500)


@app.route("/data/<path:level>/<path:file_key>/<path:file_ext>", methods=["GET"])
def handle_data_get(level, file_key, file_ext):
    object_key = f"{BUCKET_PATH_PLOTS_PREFIX}{level}/{file_key}.{file_ext}"

    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=object_key)
        file_content_bytes = response["Body"].read()
        mime = "image/png" if file_ext == "png" else "application/json"

        if file_ext == "json":
            file_content = json.loads(file_content_bytes.decode("utf-8"))
            return jsonify(file_content), 200
        else:
            return send_file(io.BytesIO(file_content_bytes), mimetype=mime), 200

    except s3_client.exceptions.NoSuchKey:
        abort(404)
    except Exception as e:
        print(f"Error accessing S3: {e}")
        abort(500)


def update_json_in_s3(object_key, new_data):
    try:
        json_string = json.dumps(new_data)
        print(json_string)
        json_bytes = json_string.encode("utf-8")

        response = s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=object_key,
            Body=json_bytes,
            ContentType="application/json",
        )

        print(response)

        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            print(f"Successfully updated '{object_key}' in '{BUCKET_NAME}'.")
            return True
        else:
            print(f"Failed to update '{object_key}'. Response: {response}")
            return False

    except Exception as e:
        print(f"Error updating JSON in S3: {e}")
        return False


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 4000))
    app.run(host="0.0.0.0", port=port, threaded=True, debug=True)
