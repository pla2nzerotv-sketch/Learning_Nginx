import http from 'k6/http';
import { check, sleep } from 'k6';

export let options = {
  vus: 10000, // количество виртуальных пользователей
  duration: '30s', // продолжительность теста
};

export default function () {
  http.get('http://localhost:8080');
}