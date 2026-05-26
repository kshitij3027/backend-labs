"""Application services — pure logic that sits between the routes and the DAO.

Services here don't own HTTP concerns (they take a session and return
plain data); the route layer wraps them in Pydantic responses.
"""
