import subprocess

def upload_to_hdfs(local_path, hdfs_path):
    try:
        subprocess.run(["hdfs", "dfs", "-put", local_path, hdfs_path], check=True)
        print(f"File {local_path} successfully uploaded to {hdfs_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error uploading file to HDFS: {e}")

upload_to_hdfs("crolling_data_20.xlsx", "/user/maria_dev/data_collection/")
