import streamlit as st
import pandas as pd
import requests
import io

# Настройки страницы
st.set_page_config(page_title="Cloud Analog Search", layout="centered")

URL = "https://docs.google.com/spreadsheets/d/1qviJPyDXzN_DKPD1tVMdsPW_IVl-3Fn2yQtEzuK0XFc/export?format=xlsx"


# Функция загрузки данных с кэшированием (чтобы не качать таблицу при каждом нажатии кнопки)
@st.cache_data(ttl=600)  # Обновлять кэш раз в 10 минут
def load_data():
    try:
        response = requests.get(URL)
        response.raise_for_status()
        df = pd.read_excel(io.BytesIO(response.content))
        df.columns = [str(col).strip() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки базы: {e}")
        return None


# Заголовок
st.title("🔄 Cloud Analog Search")

# Загрузка данных
df_global = load_data()

if df_global is not None:
    st.success(f"База обновлена (строк: {len(df_global)})")

    # Поле поиска
    query = st.text_input("Введите артикул:", placeholder="Например: 12345...").strip()

    if query:
        query_lower = query.lower()
        results = []

        # Логика поиска
        for _, row in df_global.iterrows():
            row_values = [str(v).strip() for v in row.values if pd.notna(v)]
            if any(query_lower == v.lower() for v in row_values):
                for col in df_global.columns:
                    val = str(row[col]).strip()
                    if val.lower() != query_lower and val not in ["nan", "", "None"]:
                        results.append({"Производитель": col, "Аналог": val})

        # Вывод результатов
        if results:
            st.write("### Найденные аналоги:")
            res_df = pd.DataFrame(results)
            st.table(res_df)  # Или st.dataframe(res_df) для интерактивной таблицы
        else:
            st.warning("Ничего не найдено")

# Кнопка ручного обновления
if st.button("🔄 Обновить базу вручную"):
    st.cache_data.clear()
    st.rerun()