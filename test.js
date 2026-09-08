import http from 'k6/http';

export const options = {
    vus: 100,
    duration: '2m'
}

export default function () {
    let url = 'http://localhost:8080/todos/d0bfb38f-39b8-4540-b117-3990cd745ef7';
    let response = http.get(url);
}