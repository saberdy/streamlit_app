#!/usr/bin/env python3

from typing import Literal, Dict

import logging
import duckdb
import streamlit as st
import matplotlib.pyplot as plt

from timeit import default_timer as timer

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s --- %(levelname)s --- %(message)s",
    handlers=[logging.StreamHandler()],
)


def set_page_config(
    title: str = "A Bookstore Interactive Dashboard",
    format_layout: Literal["wide", "centered"] = "wide",
    icon: str = " ",
    sidebar_state: Literal["expanded", "collapsed"] = "expanded",
) -> None:
    """
    Initialize the page with given configurations and layout

    [Parameters]
    - title (str)
    - format_layout (str)
    - icon (str)
    - sidebar_state (str)

    [Returns]
    None
    """
    st.set_page_config(
        page_title=title,
        layout=format_layout,
        page_icon=icon,
        initial_sidebar_state=sidebar_state,
    )
    st.title(title)


def set_expander_about_source(
    expander_title: str = "About the data",
    source_url: str = "https://books.toscrape.com/",
    image_url: str = (
        "https://cdn.pixabay.com/photo/2015/10/30/10/40/books-1013663_960_720.jpg"
    ),
    image_width: int = 250,
) -> None:
    """
    Display a short description about resources and general
    info of the dashboard

    [Parameters]
    - expander_title (str):
    - source_url (str):
    - image_url (str):
    - image_width (int):

    [Returns]
    None
    """
    with st.expander(expander_title):
        st.write("Source:")
        st.write(source_url)
        st.image(image_url, width=image_width)
        st.write("Photo:")
        st.write(image_url)


def select_availability_and_stars_filters(
    con: duckdb.DuckDBPyConnection, filter_title: str = "Filters"
) -> tuple:
    """
    Filter the result of preview data with the selected availability and stars
    value from dropdown menu

    [Parameters]
    - con: duckdb connection object
    - filter_title (str): title of the filter dropdown

    [Returns]
    - tuple: a tuple of two int values below
    -- availability (int)
    -- stars (int)
    """
    st.subheader(filter_title)
    col_a1, col_a2 = st.columns(2)
    with col_a1:
        availability_df = con.execute(
            """
                SELECT
                   DISTINCT availability
                FROM books_wh_tbl
                ORDER BY 1
            """
        ).df()
        availability = st.selectbox("availability", availability_df)

    with col_a2:
        stars_df = con.execute(
            """
                SELECT
                   DISTINCT stars
                FROM books_wh_tbl
                ORDER BY 1
            """
        ).df()
        stars = st.selectbox("rating stars", stars_df)
    return availability, stars


def get_number_of_matched_books(
    con: duckdb.DuckDBPyConnection, availability: int, stars: int
) -> int:
    """
    Apply the chosen filters on availability and stars to get
    the number of matched books.

    [Parameters]
    - con (ckdb.DuckDBPyConnection): duckdb connection object
    - availability (int): user's chosen available books quantity in the store
    - stars (int): user's chosen rating number from 1 to 5

    [Returns]
    - int: number of the books that matched the availability and
    stars filter chosen by the user.
    """
    result = con.execute(
        f"""
            SELECT
            COUNT(*)
            FROM books_wh_tbl
            WHERE availability = '{availability}'
                AND stars = '{stars}'
        """
    ).fetchone()
    return result[0]


def determine_price_range(con: duckdb.DuckDBPyConnection, availability, stars):
    """
    Find the min and max bound of the book prices for the given filter choices
    of availability and stars

    [Parameters]
    - con (ckdb.DuckDBPyConnection): duckdb connection object
    - availability (int): user's chosen available books quantity in the store
    - stars (int): user's chosen rating number from 1 to 5

    [Returns]
    - price range (tuple)
    -- price lower bound
    -- price upper bound
    """
    (price_lower_bound, price_upper_bound) = con.execute(
        f"""
            SELECT
                min(price),
                max(price)
            FROM books_wh_tbl
            WHERE availability = '{availability}'
                AND stars = '{stars}'
        """
    ).fetchone()
    return price_lower_bound, price_upper_bound


def select_price_range_slider(price_lower_bound, price_upper_bound):
    """
    Provide a slider to set a desired price range for the books with chosen filter
    values of availability and stars

    [Parameters]
    - price_lower_bound (float): the min price value that could be queried
    - price_upper_bound (float): the max price value that could be queried

    [Raises]
    - Exception: An unexpected error could have caused an invalid slider selection

    [Returns]
    - slider_price_min (float): a selected min book price value by the user
    - slider_price_max (float): a selected max book price value by the user
    """
    try:
        (slider_price_min, slider_price_max) = st.slider(
            "Price Range",
            min_value=price_lower_bound,
            max_value=price_upper_bound,
            value=(price_lower_bound, price_upper_bound),
        )
    except Exception as e:
        logging.error(f"An unexpected error occured: {e}")
        st.error("Invalid price range: minimum price must be less than maximum price.")

    return slider_price_min, slider_price_max


def matched_results_dataframe(
    con: duckdb.DuckDBPyConnection,
    availability: int,
    stars: int,
    slider_price_min: float,
    slider_price_max: float,
) -> dict:
    """
    Search for the matching books based on the all filter values
    provided by the user i.e. availability, stars, price range

    [Parameters]
    - con (ckdb.DuckDBPyConnection): duckdb connection object
    - availability (int): user's chosen available books quantity in the store
    - stars (int): user's chosen rating number from 1 to 5
    - price_lower_bound (float): the min price value that could be queried
    - price_upper_bound (float): the max price value that could be queried

    [Returns]
    matched results (dict): matched books for provided filters
    matched results number (int): matched books number for provided filters
    """
    query_data_preview = f"""
        SELECT *
        FROM books_wh_tbl
        WHERE availability = '{availability}'
            AND stars = '{stars}'
            AND price BETWEEN '{slider_price_min}' AND '{slider_price_max}'
        """
    matched_results = con.execute(query_data_preview).fetchdf()
    matched_results_count = matched_results.shape[0]

    return {
        "results": matched_results,
        "results_count": matched_results_count,
    }


def avg_price_from_db(
    con: duckdb.DuckDBPyConnection,
    availability: int,
    stars: int,
    slider_price_min: float,
    slider_price_max: float,
) -> float:
    """
    [Parameters]
    - con (ckdb.DuckDBPyConnection): duckdb connection object
    - availability (int): user's chosen available books quantity in the store
    - stars (int): user's chosen rating number from 1 to 5
    - slider_price_min (float): the min price value selected by the user
    - slider_price_max (float): the max price value selected by the user

    [Returns]
    avg_price (float): average book price for the given query
    """
    st.subheader("Average Book Price")
    avg_price = con.execute(
        f"""
            SELECT
                ROUND(AVG(price),2) as avg_price
            FROM books_wh_tbl
            WHERE availability = '{availability}'
                AND stars = '{stars}'
                AND price BETWEEN '{slider_price_min}' AND '{slider_price_max}'
            """
    ).fetchone()[0]
    return avg_price


def plot_results(result: Dict):
    """
    Plot the results. Display the number of books in different
    price category.

    [Parameters]
    - result (Dict): matched book price results
    """
    matched_df = result["price"]
    st.subheader("Books Distribution per Price Categories")
    plt.style.use("dark_background")
    plt.hist(matched_df.values, bins=10)
    plt.xlabel("Price Category")
    plt.ylabel("Books per Category")
    plt.title("Number of Books Found In Each Price Category")
    st.pyplot(plt)


def render_results_gte_2(
    con: duckdb.DuckDBPyConnection,
    availability: int,
    stars: int,
) -> None:
    """
    Return the result(s) of the match for selected availability and stars

    [Parameters]
    - con (ckdb.DuckDBPyConnection): duckdb connection object
    - availability (int): user's chosen available books quantity in the store
    - stars (int): user's chosen rating number from 1 to 5

    [return]
    None
    """
    price_lower_bound, price_upper_bound = determine_price_range(
        con, availability, stars
    )
    slider_price_min, slider_price_max = select_price_range_slider(
        price_lower_bound, price_upper_bound
    )
    st.text("Total Number of Books with:")
    st.text(
        f"{'Availability:':<30}{availability:>30}\n"
        f"{'Stars:':<30}{stars:>30}\n"
        f"{slider_price_min:<12}{'<':<12}"
        f"{'price':^12}"
        f"{'<':>12}{slider_price_max:>12}"
    )
    results = matched_results_dataframe(
        con, availability, stars, slider_price_min, slider_price_max
    )
    matched_results, matched_results_count = (
        results["results"],
        results["results_count"],
    )
    st.markdown(f"## {matched_results_count} Book(s) Found")
    # in the code block below, decide how many results should be displayed
    col_submit_quantity = st.columns([2, 8])
    with col_submit_quantity[0]:
        st.text("Number of Books to Display:")
        form_options = {"All": matched_results_count, 1: 1, 5: 5, 10: 10, "None": 0}
        selected_quantity = st.radio("", options=form_options.keys())
        quantity = form_options[selected_quantity]
    st.write(f"Display {quantity} results: ")
    matched_results.index = range(1, len(matched_results) + 1)
    st.dataframe(matched_results.head(quantity))
    avg_price = avg_price_from_db(
        con, availability, stars, slider_price_min, slider_price_max
    )
    st.metric(label="", value=avg_price)
    plot_results(results["results"])


def render_null_result():
    """
    Show a message to the user and indicate that there is no match
    for the given filter values.

    [Parameters]
    None

    [Returns]
    None
    """
    st.markdown("## No Matching Results.")


def render_single_result(
    con: duckdb.DuckDBPyConnection,
    availability: int,
    stars: int,
) -> None:
    """
    Show the only matched book returned from the query with availabilit
    and stars.

    [Parameters]
    - con (ckdb.DuckDBPyConnection): duckdb connection object
    - availability (int): user's chosen available books quantity in the store
    - stars (int): user's chosen rating number from 1 to 5

    [Returns]
    None
    """
    st.markdown("## Single book found:")
    query_data_preview = f"""
        SELECT *
        FROM books_wh_tbl
        WHERE availability = '{availability}'
            AND stars = '{stars}'
        """
    single_result = con.execute(query_data_preview).fetchdf()
    single_result.index = range(1, len(single_result) + 1)
    st.write(single_result)

    return None


def main() -> None:
    """
    The main entry point of the streamlit dashboard app.

    Usage:
    To run the dashboard app, run the following command:
    `streamlit run __file__`
    where the file name is `books_app.py`
    when writing this document.

    [Raises]
    - FileNotFoundError: When there is no database file to connect.
    - Exception: When an unexpected error occured

    [Parameters]
    None

    [Returns]
    None
    """
    set_page_config()
    set_expander_about_source()
    try:
        with duckdb.connect("books_wh.duckdb") as con:
            availability, stars = select_availability_and_stars_filters(con)
            try:
                number_of_found_results = get_number_of_matched_books(
                    con, availability, stars
                )
            except Exception as e:
                logging.error(f"An unexpected error occured: {e}")
                st.subheader("didn't succeed finding any match!")
                st.error(f"error occured with initial filters: \n{e}")
                raise e
            else:
                # the function lookup is equivalent to the if-else block below,
                # it checks for the number of matches to the filtered query and
                # based on that number calls a different rendering function. This approach
                # helps to keep the both app source code and app render clean.
                # if number_of_found_results >= 2:
                #     render_results_gte_2(con, availability, stars)
                # elif number_of_found_results == 1:
                #     render_single_result()
                # elif number_of_found_results == 0:
                #     render_null_result()
                function_lookup = {
                    0: render_null_result,
                    1: lambda con=con, availability=availability, stars=stars: render_single_result(
                        con, availability, stars
                    ),
                    2: lambda con=con, availability=availability, stars=stars: render_results_gte_2(
                        con, availability, stars
                    ),
                }
                function_lookup[min(2, number_of_found_results)]()
    except FileNotFoundError:
        logging.error("duckdb file is not found!")
    except Exception as e:
        logging.error(f"An unexpected error occured: {e}")

    return None


if __name__ == "__main__":
    start_the_app = timer()
    try:
        main()
    except Exception as e:
        logging.error(f"An unexpected error occured: {e}")
        st.error(e)
        raise e
    end_timer = timer()
    # display the overall time it took to run the whole app
    st.write("Total running time: ", end_timer - start_the_app, " seconds")
