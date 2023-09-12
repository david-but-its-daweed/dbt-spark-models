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
'неразобранное' as status_name
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57209870 as status_id,
'ожидают обработки' as status_name
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57244902 as status_id,
'взяли в работу' as status_name
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57244906 as status_id,
'лид квалифицирован' as status_name
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57209874 as status_id,
'заявка на расчет' as status_name
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57244910 as status_id,
'отправлен запрос биздеву' as status_name
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57209878 as status_id,
'отправлен запрос брокеру' as status_name
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57373262 as status_id,
'кп отправлено' as status_name
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57373266 as status_id,
'получена обратная связь' as status_name
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57373270 as status_id,
'переговоры' as status_name
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57373274 as status_id,
'финальный расчет' as status_name
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57373282 as status_id,
'подписание договора' as status_name
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
57373286 as status_id,
'решение отложено' as status_name
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
142 as status_id,
'успешно реализовано (оплата)' as status_name
union all
select 6769178 as pipeline_id,
'SDR' as pipeline_name,
143 as status_id,
'закрыто и не реализовано' as status_name
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52640491 as status_id,
'неразобранное' as status_name
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52640494 as status_id,
'ожидают обработки' as status_name
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
57432194 as status_id,
'лид квалифицирован' as status_name
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52640497 as status_id,
'заявка на расчет' as status_name
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52640500 as status_id,
'отправлен запрос биздеву' as status_name
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52640503 as status_id,
'отправлен запрос брокеру' as status_name
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52732306 as status_id,
'кп отправлено' as status_name
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52732309 as status_id,
'получена обратная связь' as status_name
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52969818 as status_id,
'переговоры' as status_name
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52969822 as status_id,
'финальный расчет' as status_name
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
52969830 as status_id,
'подписание договора' as status_name
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
53409454 as status_id,
'решение отложено' as status_name
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
142 as status_id,
'успешно реализовано (оплата)' as status_name
union all
select 6068353 as pipeline_id,
'Исходящие заявки' as pipeline_name,
143 as status_id,
'закрыто и не реализовано' as status_name
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка банк' as pipeline_name,
52895926 as status_id,
'неразобранное' as status_name
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка банк' as pipeline_name,
56268846 as status_id,
'ожидают обработки' as status_name
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка банк' as pipeline_name,
56268850 as status_id,
'взяли в работу' as status_name
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка банк' as pipeline_name,
56268854 as status_id,
'лид квалифицирован' as status_name
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка банк' as pipeline_name,
52895930 as status_id,
'заявка на расчет' as status_name
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка банк' as pipeline_name,
52895934 as status_id,
'отправлен запрос биздеву' as status_name
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка банк' as pipeline_name,
52895938 as status_id,
'отправлен запрос брокеру' as status_name
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка банк' as pipeline_name,
52922370 as status_id,
'запросы получены' as status_name
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка банк' as pipeline_name,
52922378 as status_id,
'кп отправлено' as status_name
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка банк' as pipeline_name,
52922382 as status_id,
'получена обратная связь' as status_name
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка банк' as pipeline_name,
53003554 as status_id,
'переговоры' as status_name
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка банк' as pipeline_name,
53003558 as status_id,
'финальный расчет' as status_name
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка банк' as pipeline_name,
53003566 as status_id,
'подписание договора' as status_name
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка банк' as pipeline_name,
53409514 as status_id,
'решение отложено' as status_name
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка банк' as pipeline_name,
142 as status_id,
'успешно реализовано (оплата)' as status_name
union all
select 6107370 as pipeline_id,
'Входящие заявки Точка банк' as pipeline_name,
143 as status_id,
'закрыто и не реализовано' as status_name
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514102 as status_id,
'неразобранное' as status_name
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514106 as status_id,
'ожидают обработки' as status_name
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514110 as status_id,
'взяли в работу' as status_name
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
57864934 as status_id,
'заявка на расчет' as status_name
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514114 as status_id,
'отправлен запрос биздеву' as status_name
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514238 as status_id,
'отправлен запрос брокеру' as status_name
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514250 as status_id,
'кп отправлено' as status_name
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514254 as status_id,
'получена обратная связь' as status_name
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514258 as status_id,
'переговоры' as status_name
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514262 as status_id,
'финальный расчет' as status_name
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514270 as status_id,
'подписание договора' as status_name
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
53514274 as status_id,
'решение отложено' as status_name
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
142 as status_id,
'успешно реализовано (оплата)' as status_name
union all
select 6200230 as pipeline_id,
'Входящие заявки Mpstats' as pipeline_name,
143 as status_id,
'закрыто и не реализовано' as status_name
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537438 as status_id,
'неразобранное' as status_name
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537442 as status_id,
'ожидают обработки' as status_name
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537446 as status_id,
'взяли в работу' as status_name
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
57513618 as status_id,
'лид квалифицирован' as status_name
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
57513622 as status_id,
'заявка на расчет' as status_name
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537450 as status_id,
'отправлен запрос биздеву' as status_name
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537542 as status_id,
'отправлен запрос брокеру' as status_name
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537554 as status_id,
'кп отправлено' as status_name
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537558 as status_id,
'получена обратная связь' as status_name
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537562 as status_id,
'переговоры' as status_name
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537566 as status_id,
'финальный расчет' as status_name
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537574 as status_id,
'подписание договора' as status_name
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
55537578 as status_id,
'решение отложено' as status_name
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
142 as status_id,
'успешно реализовано (оплата)' as status_name
union all
select 6518530 as pipeline_id,
'Входящие заявки Uforce.pro' as pipeline_name,
143 as status_id,
'закрыто и не реализовано' as status_name
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189302 as status_id,
'неразобранное' as status_name
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56269810 as status_id,
'ожидают обработки' as status_name
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56241882 as status_id,
'взяли в работу' as status_name
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56250534 as status_id,
'лид квалифицирован' as status_name
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189306 as status_id,
'заявка на расчет' as status_name
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189314 as status_id,
'отправлен запрос биздеву' as status_name
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189390 as status_id,
'отправлен запрос брокеру' as status_name
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189394 as status_id,
'запросы получены' as status_name
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189398 as status_id,
'кп готово' as status_name
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189402 as status_id,
'кп отправлено' as status_name
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189406 as status_id,
'получена обратная связь' as status_name
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189410 as status_id,
'переговоры' as status_name
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189414 as status_id,
'финальный расчет' as status_name
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
56189422 as status_id,
'подписание договора' as status_name
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
142 as status_id,
'успешно реализовано (оплата)' as status_name
union all
select 6616182 as pipeline_id,
'Не клиент Точка банк' as pipeline_name,
143 as status_id,
'закрыто и не реализовано' as status_name
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56195126 as status_id,
'неразобранное' as status_name
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56195130 as status_id,
'первичный контакт' as status_name
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56195134 as status_id,
'отправлен запрос брокеру' as status_name
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56195138 as status_id,
'отправлен запрос биздеву' as status_name
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56880354 as status_id,
'кп отправлено' as status_name
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56880358 as status_id,
'получена обратная связь' as status_name
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56880362 as status_id,
'переговоры' as status_name
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56880366 as status_id,
'финальный расчет' as status_name
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
56880374 as status_id,
'подписание договора' as status_name
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
142 as status_id,
'успешно реализовано (оплата)' as status_name
union all
select 6617058 as pipeline_id,
'Повторные заказы' as pipeline_name,
143 as status_id,
'закрыто и не реализовано' as status_name
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283770 as status_id,
'неразобранное' as status_name
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283774 as status_id,
'ожидают обработки' as status_name
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283826 as status_id,
'взяли в работу' as status_name
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283830 as status_id,
'лид квалифицирован' as status_name
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283834 as status_id,
'заявка на расчет' as status_name
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283842 as status_id,
'отправлен запрос биздеву' as status_name
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283846 as status_id,
'отправлен запрос брокеру' as status_name
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283850 as status_id,
'запросы получены' as status_name
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283858 as status_id,
'кп отправлено' as status_name
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283862 as status_id,
'получена обратная связь' as status_name
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283778 as status_id,
'переговоры' as status_name
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283782 as status_id,
'финальный расчет' as status_name
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283870 as status_id,
'подписание договора' as status_name
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
56283874 as status_id,
'решение отложено' as status_name
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
142 as status_id,
'успешно реализовано (оплата)' as status_name
union all
select 6630654 as pipeline_id,
'Email-рассылка' as pipeline_name,
143 as status_id,
'закрыто и не реализовано' as status_name
union all
select 6831498 as pipeline_id,
'Реферальная программа' as pipeline_name,
57631090 as status_id,
'неразобранное' as status_name
union all
select 6831498 as pipeline_id,
'Реферальная программа' as pipeline_name,
57631094 as status_id,
'первичный контакт' as status_name
union all
select 6831498 as pipeline_id,
'Реферальная программа' as pipeline_name,
57631098 as status_id,
'взят в работу' as status_name
union all
select 6831498 as pipeline_id,
'Реферальная программа' as pipeline_name,
57875930 as status_id,
'переговоры' as status_name
union all
select 6831498 as pipeline_id,
'Реферальная программа' as pipeline_name,
57633250 as status_id,
'промокод' as status_name
union all
select 6831498 as pipeline_id,
'Реферальная программа' as pipeline_name,
57878358 as status_id,
'отправить промокод' as status_name
union all
select 6831498 as pipeline_id,
'Реферальная программа' as pipeline_name,
142 as status_id,
'участник' as status_name
union all
select 6831498 as pipeline_id,
'Реферальная программа' as pipeline_name,
143 as status_id,
'закрыто и не реализовано' as status_name
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846662 as status_id,
'неразобранное' as status_name
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846666 as status_id,
'ожидают обработки' as status_name
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846670 as status_id,
'взяли в работу' as status_name
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846846 as status_id,
'лид квалифицирован' as status_name
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846850 as status_id,
'заявка на расчет' as status_name
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846854 as status_id,
'отправлен запрос биздеву' as status_name
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846858 as status_id,
'отправлен запрос брокеру' as status_name
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846870 as status_id,
'кп отправлено' as status_name
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846874 as status_id,
'получена обратная связь' as status_name
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846878 as status_id,
'финальный расчет' as status_name
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846882 as status_id,
'подписание договора' as status_name
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
57846674 as status_id,
'решение отложено' as status_name
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
142 as status_id,
'успешно реализовано (оплата)' as status_name
union all
select 6863702 as pipeline_id,
'Актион' as pipeline_name,
143 as status_id,
'закрыто и не реализовано' as status_name
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57849082 as status_id,
'неразобранное' as status_name
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57849086 as status_id,
'ожидают обработки' as status_name
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57878514 as status_id,
'взяли в работу' as status_name
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57878518 as status_id,
'лид квалифицирован' as status_name
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57849090 as status_id,
'заявка на расчет' as status_name
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57878522 as status_id,
'отправлен запрос биздеву' as status_name
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57878526 as status_id,
'отправлен запрос брокеру' as status_name
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57878538 as status_id,
'кп отправлено' as status_name
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57878542 as status_id,
'получена обратная связь' as status_name
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57878546 as status_id,
'финальный расчет' as status_name
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57878550 as status_id,
'подписание договора' as status_name
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
57849094 as status_id,
'решение отложено' as status_name
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
142 as status_id,
'успешно реализовано (оплата)' as status_name
union all
select 6864026 as pipeline_id,
'Актион Читатели' as pipeline_name,
143 as status_id,
'закрыто и не реализовано' as status_name
