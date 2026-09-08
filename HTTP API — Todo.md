## Ресурс

`Todo`:

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "title": "Buy milk",
  "completed": false,
  "createdAt": "2026-09-08T12:00:00Z",
  "updatedAt": "2026-09-08T12:00:00Z"
}
```

## Endpoints

### `GET /todos`

Получить список всех Todo.

**Response `200 OK`:**

```json
[
  {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "title": "Buy milk",
    "completed": false,
    "createdAt": "2026-09-08T12:00:00Z",
    "updatedAt": "2026-09-08T12:00:00Z"
  }
]
```

Сортировка: `createdAt DESC`.

---

### `POST /todos`

Создать Todo.

**Request:**

```json
{
  "title": "Buy milk"
}
```

**Response `201 Created`:**

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "title": "Buy milk",
  "completed": false,
  "createdAt": "2026-09-08T12:00:00Z",
  "updatedAt": "2026-09-08T12:00:00Z"
}
```

Заголовок `Location` должен содержать URL созданного ресурса:

```http
Location: /todos/{id}
```

---

### `GET /todos/{id}`

Получить Todo по ID.

**Response `200 OK`:** полный объект `Todo`.

Если Todo не существует — `404 Not Found`.

---

### `PATCH /todos/{id}`

Изменить Todo.

Можно передать одно или несколько полей:

```json
{
  "title": "Buy two litres of milk",
  "completed": true
}
```

Допустимые поля: `title`, `completed`.

**Response `200 OK`:** полный обновлённый объект `Todo`.

Если Todo не существует — `404 Not Found`.

---

### `DELETE /todos/{id}`

Удалить Todo.

**Response:**

```http
204 No Content
```

Если Todo не существует — `404 Not Found`.

## Ошибки

Все ошибки возвращаются в едином формате:

```json
{
  "error": {
    "code": "TODO_NOT_FOUND",
    "message": "Todo not found"
  }
}
```

Для некорректного запроса:

```json
{
  "error": {
    "code": "INVALID_REQUEST",
    "message": "title must not be empty"
  }
}
```

Используются соответствующие HTTP-коды, например `400 Bad Request` и `404 Not Found`.

## Общие требования

- JSON используется для request/response body.
- Имена полей — `camelCase`.
- `id` — UUID.
- `createdAt` и `updatedAt` — timestamps в ISO 8601.
- При создании `completed` по умолчанию равен `false`.
- Pagination и фильтрация не требуются.
- Внутренняя архитектура и используемый фреймворк не регламентируются.
- Контракт определяется только HTTP API выше.