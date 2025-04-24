import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

session = get_active_session()

st.title("Pokemon Database Streamlit App")

# Sidebar Навигация
st.sidebar.title("Навигация")
page = st.sidebar.selectbox("Категория", [
    "Първи Поглед към Инвентара",
    "Анализ на Пазарните Тенденции (Агрегации и Групиране)"
])

# ========================
# ========================
if page == "Първи Поглед към Инвентара":

    # 1.
    st.markdown("### :blue-background[1. Цяла информация по име:]")
    st.caption("Условие: Намерете цялата информация за конкретен Pokémon по име")
    pokemon_name = st.text_input("Име на Pokemon")

    if pokemon_name:
        sql = f"""
            SELECT * FROM raw_data.pokemon_general
            WHERE name LIKE '{pokemon_name}'
        """
        df = session.sql(sql).to_pandas()
        st.dataframe(df)

    st.divider()

    # 2.
    st.markdown("### :blue-background[2. Покемони от определено поколение:]")
    st.caption("Условие: Имената и каталожните номера на Pokémon от избрано поколение")
    generation = st.number_input("Избери поколение", min_value=1, max_value=9, value=1)

    sql = f"""
        SELECT name, pokedex_number
        FROM raw_data.pokemon_general
        WHERE generation = {generation}
        ORDER BY pokedex_number
    """
    df = session.sql(sql).to_pandas()
    st.dataframe(df)

    st.divider()

    # 3.
    st.markdown("### :blue-background[3. Покемони по основен тип:]")
    st.caption("Филтрирайте по type1, подредени по име")

    types = [
        "fire", "water", "grass", "electric", "normal", "psychic",
        "fighting", "flying", "bug", "poison", "rock", "ground",
        "ghost", "steel", "ice", "dragon", "dark", "fairy"
    ]
    selected_type = st.selectbox("Избери тип:", types)

    sql = f"""
        SELECT name, type1, type2
        FROM raw_data.pokemon_general
        WHERE LOWER(type1) = '{selected_type.lower()}'
        ORDER BY name
    """
    df = session.sql(sql).to_pandas()
    st.dataframe(df)

    st.divider()

    # 4.
    st.markdown("### :blue-background[4. Най-силни покемони:]")
    st.caption("Топ 5 Pokémon с най-висока attack стойност")

    sql = """
        SELECT name, attack
        FROM raw_data.pokemon_stats
        ORDER BY attack DESC
        LIMIT 5
    """
    df = session.sql(sql).to_pandas()
    df.columns = ['Name', 'Attack']
    st.bar_chart(data=df, x='Name', y='Attack', horizontal=True)

    st.divider()

    # 5. 
    st.markdown("### :blue-background[5. Редки легендарни покемони:]")
    st.caption("Намерете всички Легендарни Pokémon и пребройте ги")

    if st.button("Show legendary Pokémon"):
        sql = """
            SELECT name, generation
            FROM raw_data.pokemon_general
            WHERE is_legendary = 1
        """
        df = session.sql(sql).to_pandas()
        st.write(f"Total Legendary Pokémon: {len(df)}")
        st.dataframe(df)

    st.divider()

    # 6. 
    st.markdown("### :blue-background[6. Покемони с един тип:]")
    st.caption("Открийте Pokémon, които имат само type1")

    sql = """
        SELECT name, type1
        FROM raw_data.pokemon_general
        WHERE type2 IS NULL
        ORDER BY name
    """
    df = session.sql(sql).to_pandas()
    st.dataframe(df)


# ================================
# ================================
else:
    # 1.
    st.markdown("### :green-background[1. Брой покемони по поколения:]")
    sql = """
        SELECT generation, COUNT(*) AS pokemon_count
        FROM raw_data.pokemon_general
        GROUP BY generation
        ORDER BY generation
    """
    df = session.sql(sql).to_pandas()
    df.columns = ["Generation", "Pokemon count"]
    st.bar_chart(data=df, x="Generation", y="Pokemon count", color="#527562")

    st.divider()

    # 2. 
    st.markdown("### :green-background[2. Средностатистически данни по тип:]")
    sql = """
        SELECT
            g.type1,
            AVG(s.attack) AS avg_attack,
            AVG(g.defense) AS avg_defense,
            AVG(g.speed) AS avg_speed
        FROM raw_data.pokemon_general g
        JOIN raw_data.pokemon_stats s ON g.name = s.name
        GROUP BY g.type1
        ORDER BY avg_attack DESC
    """
    df = session.sql(sql).to_pandas()
    df.columns = ['Type1', 'Average Attack', 'Average Defense', 'Average Speed']
    st.dataframe(df)
    st.bar_chart(data=df, x='Type1', y=['Average Attack', 'Average Defense', 'Average Speed'], horizontal=True)

    st.divider()

    # 3. 
    st.markdown("### :green-background[3. Най-висок average base total :]")
    sql = """
        SELECT g.type1, AVG(g.base_total) AS avg_base_total
        FROM raw_data.pokemon_general g
        GROUP BY g.type1
        ORDER BY avg_base_total DESC
        LIMIT 1
    """
    df = session.sql(sql).to_pandas()
    df.columns = ['Type1', 'Average Base Total']
    st.dataframe(df)

    st.divider()

    # 4. C
    st.markdown("### :green-background[4. Брой по класификация :]")
    sql = """
        SELECT classification, COUNT(*) AS count
        FROM raw_data.pokemon_general
        GROUP BY classification
        ORDER BY count DESC
    """
    df = session.sql(sql).to_pandas()
    st.dataframe(df)

    st.divider()

    # 5. 
    st.markdown("### :green-background[5. Бр. легендарни за всяко поколение :]")
    sql = """
        SELECT generation, COUNT(*) AS legendary_count
        FROM raw_data.pokemon_general
        WHERE is_legendary = 1
        GROUP BY generation
        ORDER BY generation
    """
    df = session.sql(sql).to_pandas()
    df.columns = ['Generation', 'Legendary Count']
    st.bar_chart(data=df, x='Generation', y='Legendary Count')

    st.divider()

    # 6. 
    st.markdown("### :green-background[6. HP >= 75 :]")
    sql = """
        SELECT g.type1, AVG(g.hp) AS avg_hp
        FROM raw_data.pokemon_general g
        GROUP BY g.type1
        HAVING AVG(g.hp) > 75
        ORDER BY avg_hp DESC
    """
    df = session.sql(sql).to_pandas()
    st.dataframe(df)
