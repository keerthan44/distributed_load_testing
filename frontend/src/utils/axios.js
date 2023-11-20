import axios from 'axios'

export default axios.create({
  // baseURL: "http://192.168.29.206:8001/api/method/",
  baseURL: "http://127.0.0.1:8001/",
  contentType: 'application/json',
  dataType: 'application/json',
  setTimeout: 3000,
});