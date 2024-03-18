#!/usr/bin/env python
# coding: utf-8

import duckdb
import streamlit as st
from timeit import default_timer as timer

start_the_app = timer()

con = duckdb.connect("books_dwh.duckdb")
st.set_page_config(layout="wide")
st.title('Books E-commerce')
with st.expander('About the data'):
    st.write('Source: https://books.toscrape.com/')
    st.image(
        'https://cdn.pixabay.com/photo/2015/10/30/10/40/books-1013663_960_720.jpg', width=250)
    st.write(
        'Photo: https://cdn.pixabay.com/photo/2015/10/30/10/40/books-1013663_960_720.jpg')

st.subheader('Filters')
col_a1, col_a2 = st.columns(2)
with col_a1:
    availability_df = con.execute("""
        SELECT
           DISTINCT availability
        FROM books_dwh_tbl
        ORDER BY 1
    """).df()
    availability = st.selectbox('availability', availability_df)

with col_a2:
    stars_df = con.execute("""
        SELECT
           DISTINCT stars
        FROM books_dwh_tbl
        ORDER BY 1
    """).df()
    stars = st.selectbox('rating stars', stars_df)

(price_min, price_max) = con.execute("""
    SELECT
        min(price),
        max(price)
    FROM books_dwh_tbl
    WHERE availability = ?
        AND stars = ?
""", [availability, stars]).fetchone()

(slider_min, slider_max) = st.slider(
    "Price Range",
    min_value=price_min,
    max_value=price_max,
    value=(price_min, price_max),
)
st.write("Selected Range: ", slider_min, slider_max)
# main
st.subheader('Data preview')
main_table_count = con.execute("""
    SELECT COUNT(*)
    FROM books_dwh_tbl
    WHERE availability = ?
        AND stars = ?
        AND price BETWEEN ? AND ?
    """, [availability, stars, slider_min, slider_max]).fetchone()[0]

main_table_head = con.execute("""
    SELECT *
    FROM books_dwh_tbl
    WHERE availability = ?
        AND stars = ?
        AND price BETWEEN ? AND ?
    LIMIT 10
    """, [availability, stars, slider_min, slider_max]).df()
st.write('Total number of results: ', main_table_count)
st.write('Display up to 10 results: ', main_table_head)
# aggregation
st.subheader('Average Book Price')
avg_price = con.execute("""
        SELECT
            stars,
            ROUND(AVG(price),2) as avg_price
        FROM books_dwh_tbl
        WHERE stars = ?
            AND price BETWEEN ? AND ?
        GROUP BY ALL
        ORDER BY 2
        """, [stars, slider_min, slider_max]).df()
st.metric(label=f'Average book price for the review rating of {stars} stars:',
          value=avg_price['avg_price'])
# chart
st.subheader(f'Price per Book for the review rating of {stars} stars')
main_table_head_20 = con.execute("""
    SELECT *
    FROM books_dwh_tbl
    WHERE stars = ?
        AND price BETWEEN ? AND ?
    """, [stars, slider_min, slider_max]).df()

st.bar_chart(main_table_head_20, y=[
    "price", 'stars'], x="title", color=["#FF0000", "#008080"])

con.close()
end_timer = timer()
st.write("Total running time: ", end_timer-start_the_app, " seconds")
