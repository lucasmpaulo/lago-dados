SELECT
    DISTINCT IdProduto,
    case
        when IdProduto = 1  then 'Presença Streak'
        when IdProduto = 2  then 'Resgatar Ponei'
        when IdProduto = 3  then 'Churn_2pp'
        when IdProduto = 4  then 'Trocar pontos Stream'
        when IdProduto = 5  then 'R Lover'
        when IdProduto = 6  then 'Churn_10pp'
        when IdProduto = 7  then 'Bolo'
        when IdProduto = 8  then 'Coca-Cola'
        when IdProduto = 9  then 'Lista de Presença'
        when IdProduto = 10 then 'Daily Loot'
        when IdProduto = 11 then 'Churn_5pp'
        when IdProduto = 12 then 'Batata'
        when IdProduto = 13 then 'ChatMessage'
        when IdProduto = 14 then 'Torta'
        when IdProduto = 15 then 'Airflow Lover'
        when IdProduto = 16 then 'Cerveja'
    end AS NomeProduto
FROM bronze.transactions_product
ORDER BY 1