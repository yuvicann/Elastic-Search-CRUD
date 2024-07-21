from fastapi import FastAPI, HTTPException
from elasticsearch_db import ElasticsearchWrapper

app = FastAPI()
es_wrapper = ElasticsearchWrapper()

@app.get("/students/")
def read_all_students():
    try:
        students = es_wrapper.get_all_documents("students")
        return students
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/students/")
def create_student(student_data: dict):
    student_id = student_data.get("id")
    if not student_id:
        raise HTTPException(status_code=400, detail="Student ID is required")
    try:
        es_wrapper.create_document("students", student_id, student_data)
        return {"message": "Student created successfully", "student_id": student_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/students/{student_id}")
def read_student(student_id: str):
    try:
        student = es_wrapper.get_document("students", student_id)
        if student:
            return student
        else:
            raise HTTPException(status_code=404, detail="Student not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/students/{student_id}")
def update_student(student_id: str, student_data: dict):
    try:
        es_wrapper.update_document("students", student_id, student_data)
        return {"message": "Student updated successfully", "student_id": student_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/students/{student_id}")
def delete_student(student_id: str):
    try:
        es_wrapper.delete_document("students", student_id)
        return {"message": "Student deleted successfully", "student_id": student_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
