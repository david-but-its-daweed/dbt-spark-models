{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

    
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57209866 as status_id,
'Неразобранное' as status_name,
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57209870 as status_id,
'Ожидают обработки' as status_name,
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57244902 as status_id,
'Взяли в работу' as status_name,
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57244906 as status_id,
'Лид квалифицирован' as status_name,
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
59081074 as status_id,
'Предложение отправлено SDR' as status_name,
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57209874 as status_id,
'Заявка на расчет' as status_name,
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57244910 as status_id,
'Отправлен запрос биздеву' as status_name,
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57209878 as status_id,
'ОТПРАВЛЕН ЗАПРОС БРОКЕРУ' as status_name,
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57373262 as status_id,
'КП отправлено' as status_name,
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57373266 as status_id,
'Получена обратная связь' as status_name,
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57373270 as status_id,
'Переговоры' as status_name,
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57373274 as status_id,
'Финальный расчет' as status_name,
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57373282 as status_id,
'Подписание договора' as status_name,
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57373286 as status_id,
'Решение отложено' as status_name,
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
142 as status_id,
'Успешно реализовано (оплата)' as status_name,
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 6915874 as pipeline_id,
'SDR 45%' as pipeline_name,
58200050 as status_id,
'Неразобранное' as status_name,
union all
select 6915874 as pipeline_id,
'SDR 45%' as pipeline_name,
58200054 as status_id,
'Ожидают обработки' as status_name,
union all
select 6915874 as pipeline_id,
'SDR 45%' as pipeline_name,
58200198 as status_id,
'Взяли в работу' as status_name,
union all
select 6915874 as pipeline_id,
'SDR 45%' as pipeline_name,
58200202 as status_id,
'Лид квалифицирован' as status_name,
union all
select 6915874 as pipeline_id,
'SDR 45%' as pipeline_name,
59113794 as status_id,
'Предложение отправлено SDR' as status_name,
union all
select 6915874 as pipeline_id,
'SDR 45%' as pipeline_name,
58200058 as status_id,
'Заявка на расчет' as status_name,
union all
select 6915874 as pipeline_id,
'SDR 45%' as pipeline_name,
58200062 as status_id,
'Отправлен запрос биздеву' as status_name,
union all
select 6915874 as pipeline_id,
'SDR 45%' as pipeline_name,
58200206 as status_id,
'Отправлен запрос брокеру' as status_name,
union all
select 6915874 as pipeline_id,
'SDR 45%' as pipeline_name,
58200210 as status_id,
'КП отправлено' as status_name,
union all
select 6915874 as pipeline_id,
'SDR 45%' as pipeline_name,
58200214 as status_id,
'Получена обратная связь' as status_name,
union all
select 6915874 as pipeline_id,
'SDR 45%' as pipeline_name,
58200218 as status_id,
'Переговоры' as status_name,
union all
select 6915874 as pipeline_id,
'SDR 45%' as pipeline_name,
58200222 as status_id,
'Финальный расчет' as status_name,
union all
select 6915874 as pipeline_id,
'SDR 45%' as pipeline_name,
58200226 as status_id,
'Подписание договора' as status_name,
union all
select 6915874 as pipeline_id,
'SDR 45%' as pipeline_name,
142 as status_id,
'Успешно реализовано (оплата)' as status_name,
union all
select 6915874 as pipeline_id,
'SDR 45%' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52640491 as status_id,
'Неразобранное' as status_name,
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52640494 as status_id,
'Ожидают обработки' as status_name,
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
57432194 as status_id,
'Лид квалифицирован' as status_name,
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52640497 as status_id,
'Заявка на расчет' as status_name,
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52640500 as status_id,
'Отправлен запрос биздеву' as status_name,
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52640503 as status_id,
'отправлен запрос брокеру' as status_name,
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52732306 as status_id,
'кп отправлено' as status_name,
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52732309 as status_id,
'получена обратная связь' as status_name,
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52969818 as status_id,
'переговоры' as status_name,
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52969822 as status_id,
'финальный расчет' as status_name,
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52969830 as status_id,
'Подписание договора' as status_name,
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
53409454 as status_id,
'решение отложено' as status_name,
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
142 as status_id,
'успешно реализовано (оплата)' as status_name,
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка Банк' as pipeline_name,
52895926 as status_id,
'Неразобранное' as status_name,
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка Банк' as pipeline_name,
56268846 as status_id,
'Ожидают обработки' as status_name,
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка Банк' as pipeline_name,
56268850 as status_id,
'Взяли в работу' as status_name,
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка Банк' as pipeline_name,
56268854 as status_id,
'Лид квалифицирован' as status_name,
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка Банк' as pipeline_name,
52895930 as status_id,
'Заявка на расчет' as status_name,
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка Банк' as pipeline_name,
52895934 as status_id,
'отправлен запрос биздеву' as status_name,
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка Банк' as pipeline_name,
52895938 as status_id,
'отправлен запрос брокеру' as status_name,
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка Банк' as pipeline_name,
52922378 as status_id,
'кп отправлено' as status_name,
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка Банк' as pipeline_name,
52922382 as status_id,
'получена обратная связь' as status_name,
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка Банк' as pipeline_name,
53003554 as status_id,
'переговоры' as status_name,
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка Банк' as pipeline_name,
53003558 as status_id,
'финальный расчет' as status_name,
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка Банк' as pipeline_name,
53003566 as status_id,
'Подписание договора' as status_name,
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка Банк' as pipeline_name,
53409514 as status_id,
'решение отложено' as status_name,
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка Банк' as pipeline_name,
142 as status_id,
'Успешно реализовано (оплата)' as status_name,
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка Банк' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514102 as status_id,
'Неразобранное' as status_name,
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514106 as status_id,
'Ожидают обработки' as status_name,
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514110 as status_id,
'взяли в работу' as status_name,
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
58279714 as status_id,
'Лид квалицирован' as status_name,
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
57864934 as status_id,
'Заявка на расчет' as status_name,
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514114 as status_id,
'отправлен запрос биздеву' as status_name,
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514238 as status_id,
'отправлен запрос брокеру' as status_name,
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514250 as status_id,
'кп отправлено' as status_name,
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514254 as status_id,
'получена обратная связь' as status_name,
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514258 as status_id,
'переговоры' as status_name,
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514262 as status_id,
'финальный расчет' as status_name,
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514270 as status_id,
'Подписание договора' as status_name,
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514274 as status_id,
'решение отложено' as status_name,
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
142 as status_id,
'Успешно реализовано (оплата)' as status_name,
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537438 as status_id,
'Неразобранное' as status_name,
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537442 as status_id,
'Ожидают обработки' as status_name,
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537446 as status_id,
'взяли в работу' as status_name,
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
57513618 as status_id,
'Лид квалифицирован' as status_name,
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
57513622 as status_id,
'Заявка на расчет' as status_name,
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537450 as status_id,
'Отправлен запрос биздеву' as status_name,
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537542 as status_id,
'отправлен запрос брокеру' as status_name,
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537554 as status_id,
'кп отправлено' as status_name,
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537558 as status_id,
'получена обратная связь' as status_name,
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537562 as status_id,
'переговоры' as status_name,
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537566 as status_id,
'финальный расчет' as status_name,
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537574 as status_id,
'Подписание договора' as status_name,
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537578 as status_id,
'решение отложено' as status_name,
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
142 as status_id,
'Успешно реализовано (оплата)' as status_name,
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189302 as status_id,
'Неразобранное' as status_name,
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56269810 as status_id,
'Ожидают обработки' as status_name,
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56241882 as status_id,
'Взяли в работу' as status_name,
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56250534 as status_id,
'Лид квалифицирован' as status_name,
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189306 as status_id,
'Заявка на расчет' as status_name,
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189314 as status_id,
'Отправлен запрос биздеву' as status_name,
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189390 as status_id,
'Отправлен запрос брокеру' as status_name,
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189402 as status_id,
'КП отправлено' as status_name,
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189406 as status_id,
'Получена обратная связь' as status_name,
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189410 as status_id,
'Переговоры' as status_name,
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189414 as status_id,
'Финальный расчет' as status_name,
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189422 as status_id,
'Подписание договора' as status_name,
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
142 as status_id,
'Успешно реализовано (оплата)' as status_name,
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56195126 as status_id,
'Неразобранное' as status_name,
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56195130 as status_id,
'Первичный контакт' as status_name,
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56195134 as status_id,
'Отправлен запрос брокеру' as status_name,
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56195138 as status_id,
'Отправлен запрос биздеву' as status_name,
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56880354 as status_id,
'КП отправлено' as status_name,
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56880358 as status_id,
'Получена обратная связь' as status_name,
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56880362 as status_id,
'Переговоры' as status_name,
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56880366 as status_id,
'Финальный расчет' as status_name,
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56880374 as status_id,
'Подписание договора' as status_name,
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
142 as status_id,
'Успешно реализовано (оплата)' as status_name,
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283770 as status_id,
'Неразобранное' as status_name,
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283774 as status_id,
'Ожидают обработки' as status_name,
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283826 as status_id,
'Взяли в работу' as status_name,
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283830 as status_id,
'Лид квалифицирован' as status_name,
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283834 as status_id,
'Заявка на расчет' as status_name,
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283842 as status_id,
'Отправлен запрос биздеву' as status_name,
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283846 as status_id,
'Отправлен запрос брокеру' as status_name,
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283858 as status_id,
'КП отправлено' as status_name,
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283862 as status_id,
'Получена обратная связь' as status_name,
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283778 as status_id,
'Переговоры' as status_name,
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283782 as status_id,
'Финальный расчет' as status_name,
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283870 as status_id,
'Подписание договора' as status_name,
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283874 as status_id,
'Решение отложено' as status_name,
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
142 as status_id,
'Успешно реализовано (оплата)' as status_name,
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 6831498 as pipeline_id,
'Реферальная программа' as pipeline_name,
57631090 as status_id,
'Неразобранное' as status_name,
union all
select 6831498 as pipeline_id,
'Реферальная программа' as pipeline_name,
57631094 as status_id,
'Первичный контакт' as status_name,
union all
select 6831498 as pipeline_id,
'Реферальная программа' as pipeline_name,
57631098 as status_id,
'Взят в работу' as status_name,
union all
select 6831498 as pipeline_id,
'Реферальная программа' as pipeline_name,
57875930 as status_id,
'Переговоры' as status_name,
union all
select 6831498 as pipeline_id,
'Реферальная программа' as pipeline_name,
57633250 as status_id,
'промокод' as status_name,
union all
select 6831498 as pipeline_id,
'Реферальная программа' as pipeline_name,
57878358 as status_id,
'Отправить промокод' as status_name,
union all
select 6831498 as pipeline_id,
'Реферальная программа' as pipeline_name,
142 as status_id,
'Участник' as status_name,
union all
select 6831498 as pipeline_id,
'Реферальная программа' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846662 as status_id,
'Неразобранное' as status_name,
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846666 as status_id,
'Ожидают обработки' as status_name,
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846670 as status_id,
'Взяли в работу' as status_name,
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846846 as status_id,
'Лид квалифицирован' as status_name,
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846850 as status_id,
'Заявка на расчет' as status_name,
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846854 as status_id,
'Отправлен запрос биздеву' as status_name,
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846858 as status_id,
'Отправлен запрос брокеру' as status_name,
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846870 as status_id,
'КП отправлено' as status_name,
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846874 as status_id,
'Получена обратная связь' as status_name,
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846878 as status_id,
'Финальный расчет' as status_name,
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846882 as status_id,
'Подписание договора' as status_name,
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846674 as status_id,
'Решение отложено' as status_name,
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
142 as status_id,
'Успешно реализовано (оплата)' as status_name,
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57849082 as status_id,
'Неразобранное' as status_name,
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57849086 as status_id,
'Ожидают обработки' as status_name,
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57878514 as status_id,
'Взяли в работу' as status_name,
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57878518 as status_id,
'Лид квалифицирован' as status_name,
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57849090 as status_id,
'Заявка на расчет' as status_name,
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57878522 as status_id,
'Отправлен запрос биздеву' as status_name,
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57878526 as status_id,
'Отправлен запрос брокеру' as status_name,
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57878538 as status_id,
'КП отправлено' as status_name,
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57878542 as status_id,
'Получена обратная связь' as status_name,
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57878546 as status_id,
'Финальный расчет' as status_name,
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57878550 as status_id,
'Подписание договора' as status_name,
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57849094 as status_id,
'Решение отложено' as status_name,
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
142 as status_id,
'Успешно реализовано (оплата)' as status_name,
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 6932038 as pipeline_id,
'1seller' as pipeline_name,
58308510 as status_id,
'Неразобранное' as status_name,
union all
select 6932038 as pipeline_id,
'1seller' as pipeline_name,
58308514 as status_id,
'Ожидают обработки' as status_name,
union all
select 6932038 as pipeline_id,
'1seller' as pipeline_name,
58308518 as status_id,
'Взяли в работу' as status_name,
union all
select 6932038 as pipeline_id,
'1seller' as pipeline_name,
58308830 as status_id,
'Лид квалифицирован' as status_name,
union all
select 6932038 as pipeline_id,
'1seller' as pipeline_name,
58308834 as status_id,
'Заявка на расчет' as status_name,
union all
select 6932038 as pipeline_id,
'1seller' as pipeline_name,
58308522 as status_id,
'Отправлен запрос биздеву' as status_name,
union all
select 6932038 as pipeline_id,
'1seller' as pipeline_name,
58308838 as status_id,
'Отправлен запрос брокеру' as status_name,
union all
select 6932038 as pipeline_id,
'1seller' as pipeline_name,
58308842 as status_id,
'КП отправлено' as status_name,
union all
select 6932038 as pipeline_id,
'1seller' as pipeline_name,
58308846 as status_id,
'Получена обратная связь' as status_name,
union all
select 6932038 as pipeline_id,
'1seller' as pipeline_name,
58308850 as status_id,
'Переговоры' as status_name,
union all
select 6932038 as pipeline_id,
'1seller' as pipeline_name,
58308854 as status_id,
'Финальный расчет' as status_name,
union all
select 6932038 as pipeline_id,
'1seller' as pipeline_name,
58308858 as status_id,
'Подписание договора' as status_name,
union all
select 6932038 as pipeline_id,
'1seller' as pipeline_name,
142 as status_id,
'Успешно реализовано (оплата)' as status_name,
union all
select 6932038 as pipeline_id,
'1seller' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 7028806 as pipeline_id,
'Smarter' as pipeline_name,
58962014 as status_id,
'Неразобранное' as status_name,
union all
select 7028806 as pipeline_id,
'Smarter' as pipeline_name,
58962018 as status_id,
'Ожидают обработки' as status_name,
union all
select 7028806 as pipeline_id,
'Smarter' as pipeline_name,
58962194 as status_id,
'Взяли в работу' as status_name,
union all
select 7028806 as pipeline_id,
'Smarter' as pipeline_name,
58962198 as status_id,
'Лид квалифицирован' as status_name,
union all
select 7028806 as pipeline_id,
'Smarter' as pipeline_name,
58962202 as status_id,
'Заявка на расчет' as status_name,
union all
select 7028806 as pipeline_id,
'Smarter' as pipeline_name,
58962206 as status_id,
'Отправлен запрос биздеву' as status_name,
union all
select 7028806 as pipeline_id,
'Smarter' as pipeline_name,
58962210 as status_id,
'Отправлен запрос брокеру' as status_name,
union all
select 7028806 as pipeline_id,
'Smarter' as pipeline_name,
58962214 as status_id,
'КП отправлено' as status_name,
union all
select 7028806 as pipeline_id,
'Smarter' as pipeline_name,
58962022 as status_id,
'Получена обратная связь' as status_name,
union all
select 7028806 as pipeline_id,
'Smarter' as pipeline_name,
58962218 as status_id,
'Переговоры' as status_name,
union all
select 7028806 as pipeline_id,
'Smarter' as pipeline_name,
58962222 as status_id,
'Финальный расчет' as status_name,
union all
select 7028806 as pipeline_id,
'Smarter' as pipeline_name,
58962226 as status_id,
'Подписание договора' as status_name,
union all
select 7028806 as pipeline_id,
'Smarter' as pipeline_name,
142 as status_id,
'Успешно реализовано (оплата)' as status_name,
union all
select 7028806 as pipeline_id,
'Smarter' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 7077122 as pipeline_id,
'МТС Банк' as pipeline_name,
59291150 as status_id,
'Неразобранное' as status_name,
union all
select 7077122 as pipeline_id,
'МТС Банк' as pipeline_name,
59291154 as status_id,
'Ожидают обработки' as status_name,
union all
select 7077122 as pipeline_id,
'МТС Банк' as pipeline_name,
59291230 as status_id,
'Взяли в работу' as status_name,
union all
select 7077122 as pipeline_id,
'МТС Банк' as pipeline_name,
59291234 as status_id,
'Лид квалифицирован' as status_name,
union all
select 7077122 as pipeline_id,
'МТС Банк' as pipeline_name,
59291158 as status_id,
'Заявка на расчет' as status_name,
union all
select 7077122 as pipeline_id,
'МТС Банк' as pipeline_name,
59291238 as status_id,
'Отправлен запрос биздеву' as status_name,
union all
select 7077122 as pipeline_id,
'МТС Банк' as pipeline_name,
59291162 as status_id,
'Отправлен запрос брокеру' as status_name,
union all
select 7077122 as pipeline_id,
'МТС Банк' as pipeline_name,
59291242 as status_id,
'КП отправлено' as status_name,
union all
select 7077122 as pipeline_id,
'МТС Банк' as pipeline_name,
59291246 as status_id,
'Получена обратная связь' as status_name,
union all
select 7077122 as pipeline_id,
'МТС Банк' as pipeline_name,
59291250 as status_id,
'Переговоры' as status_name,
union all
select 7077122 as pipeline_id,
'МТС Банк' as pipeline_name,
59291254 as status_id,
'Финальный расчет' as status_name,
union all
select 7077122 as pipeline_id,
'МТС Банк' as pipeline_name,
59291258 as status_id,
'Подписание договора' as status_name,
union all
select 7077122 as pipeline_id,
'МТС Банк' as pipeline_name,
142 as status_id,
'Успешно реализовано (оплата)' as status_name,
union all
select 7077122 as pipeline_id,
'МТС Банк' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 7109794 as pipeline_id,
'Низкая логистика' as pipeline_name,
59503086 as status_id,
'Неразобранное' as status_name,
union all
select 7109794 as pipeline_id,
'Низкая логистика' as pipeline_name,
59503090 as status_id,
'База клиентов' as status_name,
union all
select 7109794 as pipeline_id,
'Низкая логистика' as pipeline_name,
59510810 as status_id,
'Взяли в работу' as status_name,
union all
select 7109794 as pipeline_id,
'Низкая логистика' as pipeline_name,
59510814 as status_id,
'Лид квалифицирован' as status_name,
union all
select 7109794 as pipeline_id,
'Низкая логистика' as pipeline_name,
59510818 as status_id,
'Заявка на расчет' as status_name,
union all
select 7109794 as pipeline_id,
'Низкая логистика' as pipeline_name,
59543378 as status_id,
'Отправлен запрос биздеву' as status_name,
union all
select 7109794 as pipeline_id,
'Низкая логистика' as pipeline_name,
59543382 as status_id,
'Отправлен запрос брокеру' as status_name,
union all
select 7109794 as pipeline_id,
'Низкая логистика' as pipeline_name,
59543386 as status_id,
'КП отправлено' as status_name,
union all
select 7109794 as pipeline_id,
'Низкая логистика' as pipeline_name,
59543390 as status_id,
'Получена обратная связь' as status_name,
union all
select 7109794 as pipeline_id,
'Низкая логистика' as pipeline_name,
59543394 as status_id,
'Переговоры' as status_name,
union all
select 7109794 as pipeline_id,
'Низкая логистика' as pipeline_name,
59543398 as status_id,
'Финальный расчет' as status_name,
union all
select 7109794 as pipeline_id,
'Низкая логистика' as pipeline_name,
59543402 as status_id,
'Подписание договора' as status_name,
union all
select 7109794 as pipeline_id,
'Низкая логистика' as pipeline_name,
142 as status_id,
'Успешно реализовано (оплата)' as status_name,
union all
select 7109794 as pipeline_id,
'Низкая логистика' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 7120174 as pipeline_id,
'Квалификация Rocket' as pipeline_name,
59575362 as status_id,
'Неразобранное' as status_name,
union all
select 7120174 as pipeline_id,
'Квалификация Rocket' as pipeline_name,
59575366 as status_id,
'Ожидают обработки' as status_name,
union all
select 7120174 as pipeline_id,
'Квалификация Rocket' as pipeline_name,
59575370 as status_id,
'Взят в работу' as status_name,
union all
select 7120174 as pipeline_id,
'Квалификация Rocket' as pipeline_name,
59575374 as status_id,
'Квалифицирован' as status_name,
union all
select 7120174 as pipeline_id,
'Квалификация Rocket' as pipeline_name,
59575518 as status_id,
'Ждем расчета' as status_name,
union all
select 7120174 as pipeline_id,
'Квалификация Rocket' as pipeline_name,
142 as status_id,
'Заявка на расчет' as status_name,
union all
select 7120174 as pipeline_id,
'Квалификация Rocket' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 7120182 as pipeline_id,
'Продажи Rocket' as pipeline_name,
59575394 as status_id,
'Неразобранное' as status_name,
union all
select 7120182 as pipeline_id,
'Продажи Rocket' as pipeline_name,
59575398 as status_id,
'Request retrieval' as status_name,
union all
select 7120182 as pipeline_id,
'Продажи Rocket' as pipeline_name,
59575402 as status_id,
'Trial pricing' as status_name,
union all
select 7120182 as pipeline_id,
'Продажи Rocket' as pipeline_name,
59575406 as status_id,
'Waiting trial pricing feedback' as status_name,
union all
select 7120182 as pipeline_id,
'Продажи Rocket' as pipeline_name,
59575582 as status_id,
'RFQ' as status_name,
union all
select 7120182 as pipeline_id,
'Продажи Rocket' as pipeline_name,
59575586 as status_id,
'Preparing sales quote' as status_name,
union all
select 7120182 as pipeline_id,
'Продажи Rocket' as pipeline_name,
59575590 as status_id,
'Waiting sales quote feedback' as status_name,
union all
select 7120182 as pipeline_id,
'Продажи Rocket' as pipeline_name,
59575594 as status_id,
'Forming order' as status_name,
union all
select 7120182 as pipeline_id,
'Продажи Rocket' as pipeline_name,
59575598 as status_id,
'Signing and waiting for payment' as status_name,
union all
select 7120182 as pipeline_id,
'Продажи Rocket' as pipeline_name,
59575602 as status_id,
'Manufacturing and delivery' as status_name,
union all
select 7120182 as pipeline_id,
'Продажи Rocket' as pipeline_name,
142 as status_id,
'Track client' as status_name,
union all
select 7120182 as pipeline_id,
'Продажи Rocket' as pipeline_name,
143 as status_id,
'Reject' as status_name,
union all
select 7120186 as pipeline_id,
'Холодный обзвон Rocket' as pipeline_name,
59575410 as status_id,
'Неразобранное' as status_name,
union all
select 7120186 as pipeline_id,
'Холодный обзвон Rocket' as pipeline_name,
59575414 as status_id,
'База' as status_name,
union all
select 7120186 as pipeline_id,
'Холодный обзвон Rocket' as pipeline_name,
59575418 as status_id,
'Взят в работу' as status_name,
union all
select 7120186 as pipeline_id,
'Холодный обзвон Rocket' as pipeline_name,
59575422 as status_id,
'Квалифицирован' as status_name,
union all
select 7120186 as pipeline_id,
'Холодный обзвон Rocket' as pipeline_name,
59575614 as status_id,
'Предложение отправлено' as status_name,
union all
select 7120186 as pipeline_id,
'Холодный обзвон Rocket' as pipeline_name,
142 as status_id,
'Заявка на расчет' as status_name,
union all
select 7120186 as pipeline_id,
'Холодный обзвон Rocket' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 7120194 as pipeline_id,
'Рефералы Rocket' as pipeline_name,
59575446 as status_id,
'Неразобранное' as status_name,
union all
select 7120194 as pipeline_id,
'Рефералы Rocket' as pipeline_name,
59575450 as status_id,
'Новая заявка' as status_name,
union all
select 7120194 as pipeline_id,
'Рефералы Rocket' as pipeline_name,
142 as status_id,
'Реф. ссылка выдана' as status_name,
union all
select 7120194 as pipeline_id,
'Рефералы Rocket' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 7120198 as pipeline_id,
'Неактивные Rocket' as pipeline_name,
59575462 as status_id,
'Неразобранное' as status_name,
union all
select 7120198 as pipeline_id,
'Неактивные Rocket' as pipeline_name,
59575466 as status_id,
'Отказники' as status_name,
union all
select 7120198 as pipeline_id,
'Неактивные Rocket' as pipeline_name,
59575470 as status_id,
'На реанимацию' as status_name,
union all
select 7120198 as pipeline_id,
'Неактивные Rocket' as pipeline_name,
59575474 as status_id,
'Отложенный спрос' as status_name,
union all
select 7120198 as pipeline_id,
'Неактивные Rocket' as pipeline_name,
142 as status_id,
'Реанимировали' as status_name,
union all
select 7120198 as pipeline_id,
'Неактивные Rocket' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 7120202 as pipeline_id,
'Прогрев Rocket' as pipeline_name,
59575478 as status_id,
'Неразобранное' as status_name,
union all
select 7120202 as pipeline_id,
'Прогрев Rocket' as pipeline_name,
59575482 as status_id,
'Касание №1' as status_name,
union all
select 7120202 as pipeline_id,
'Прогрев Rocket' as pipeline_name,
59575486 as status_id,
'Касание №2' as status_name,
union all
select 7120202 as pipeline_id,
'Прогрев Rocket' as pipeline_name,
59575490 as status_id,
'Касание №3' as status_name,
union all
select 7120202 as pipeline_id,
'Прогрев Rocket' as pipeline_name,
59575742 as status_id,
'Касание №4' as status_name,
union all
select 7120202 as pipeline_id,
'Прогрев Rocket' as pipeline_name,
59575746 as status_id,
'Касание №5' as status_name,
union all
select 7120202 as pipeline_id,
'Прогрев Rocket' as pipeline_name,
59575750 as status_id,
'Касание №6' as status_name,
union all
select 7120202 as pipeline_id,
'Прогрев Rocket' as pipeline_name,
59575754 as status_id,
'Касание №7' as status_name,
union all
select 7120202 as pipeline_id,
'Прогрев Rocket' as pipeline_name,
142 as status_id,
'Успех' as status_name,
union all
select 7120202 as pipeline_id,
'Прогрев Rocket' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
union all
select 7136646 as pipeline_id,
'База клиентов Rocket' as pipeline_name,
59687290 as status_id,
'Неразобранное' as status_name,
union all
select 7136646 as pipeline_id,
'База клиентов Rocket' as pipeline_name,
59687294 as status_id,
'База' as status_name,
union all
select 7136646 as pipeline_id,
'База клиентов Rocket' as pipeline_name,
142 as status_id,
'Успешно реализовано' as status_name,
union all
select 7136646 as pipeline_id,
'База клиентов Rocket' as pipeline_name,
143 as status_id,
'Закрыто и не реализовано' as status_name,
