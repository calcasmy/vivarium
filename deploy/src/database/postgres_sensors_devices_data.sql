--
-- Data for Name: devices; Type: TABLE DATA; Schema: public; Owner: ibis
--

COPY public.devices (device_id, device_name, device_type, location, model, date_added) FROM stdin;
1	Terrarium Light	Grow Light	Top	Update later	2024-03-01
2	Misting System	Mistking Starter Misting System	Top	v5.0 Starter Misting System	2024-03-01
3	Humidifier	Classic 300S Ultrasonic Smart Humidifier	Top	Classic 300	2024-03-01
4	Internal Fan	Noctua NF-A6x15 FLX, 12V, 3-Pin (60x15mm, Brown)	Internal	Noctua NF-A6x15 FLX	2025-06-01
5	Exhaust	Noctua NF-A4x10 5V PWM, 4-Pin, (40x10mm, Brown)	External Top	Noctua NF-A4x10 5V PWM	2025-06-01
\.


--
-- Data for Name: sensors; Type: TABLE DATA; Schema: public; Owner: ibis
--

COPY public.sensors (sensor_id, sensor_name, sensor_type, location, model, date_installed) FROM stdin;
1	Adafruit HTU21D-F	Temperature/Humidity Sensor	terrarium	Adafruit HTU21D-F	2024-04-01
\.


--
-- Name: devices_device_id_seq; Type: SEQUENCE SET; Schema: public; Owner: ibis
--

SELECT pg_catalog.setval('public.devices_device_id_seq', 5, true);


--
-- Name: sensors_sensor_id_seq; Type: SEQUENCE SET; Schema: public; Owner: ibis
--

SELECT pg_catalog.setval('public.sensors_sensor_id_seq', 1, true);