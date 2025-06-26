# Genie AI

**Genie AI** is a robust backend platform built with FastAPI, designed to manage user profiles, meetings, and personalized analytics. It integrates with external services (Google OAuth, Azure, Salesforce) and provides a comprehensive API for user management, file uploads, meeting analytics, and badge-based gamification. The backend is containerized with Docker, uses PostgreSQL for data storage, and is structured for scalable, production-grade deployments.

---

## Features

- **User & Profile Management:** Create, update, and manage user profiles and meeting data.
- **OAuth Integration:** Supports Google OAuth for secure authentication.
- **File Uploads:** Secure, tenant-aware file upload and management.
- **Meeting Analytics:** Endpoints for meeting overviews, attendee insights, and action items.
- **Badge & Gamification:** Track user progress and engagement with badges.
- **Admin Tools:** Endpoints for user management, syncing, and analytics.
- **Real-time Events:** SSE endpoints for notifications and badge updates.
- **Extensible & Modular:** Service-based architecture for easy extension.
- **Cloud Ready:** Dockerized for deployment on any cloud or container platform.

---

## Tech Stack

- **Backend:** FastAPI (Python 3.12)
- **Database:** PostgreSQL
- **Auth:** OAuth2, JWT (optional)
- **Cloud Integrations:** Google, Azure, Salesforce (optional)
- **Containerization:** Docker
- **Other:** Async support, background tasks, CORS, session middleware

---

## Getting Started

1. **Clone the repository**
2. **Install dependencies:**  
   Run `uv sync` (or use pip/poetry as per `pyproject.toml`).
3. **Set environment variables:**  
   (see `.env.example` or use `dotenv`).
4. **Run the API:**  
   Execute `python start_api.py`  
   or with Docker:  
   ```bash
   docker build -f Dockerfile.root -t genie-ai .
   docker run -p 8000:8000 genie-ai
   ```
5. **API Docs:**  
   Visit `http://localhost:8000/docs` for interactive Swagger UI.

---

## Example Endpoints

- `GET /v1/user-info/{user_id}` – Get user info
- `POST /v1/generate-upload-url` – Generate file upload URL
- `GET /v1/{user_id}/meetings` – List user meetings
- `GET /v1/{user_id}/profiles/{uuid}/strengths` – Get profile strengths
- `POST /v1/users/login-event` – Log user login event

---

## Contributing

1. Fork the repo
2. Create a feature branch
3. Submit a PR

---

