import streamlit as st
# from query_databricks import query_accessible_hotels
# from slack_notify import notify_staff

st.set_page_config(page_title="Accessible Travel Agent", layout="centered")

st.title("🏨 Accessible Travel Assistant")

city = st.text_input("Enter your destination city", value="New York")
user_email = st.text_input("Enter your email for confirmation")

if st.button("Find Accessible Hotels"):
    with st.spinner("Searching Databricks database..."):
        # df = query_accessible_hotels(city)
        print('queries')
    # if df.empty:
    #     st.warning("No accessible hotels found.")
    # else:
    #     st.success(f"Found {len(df)} accessible options!")
    #     for i, row in df.iterrows():
    #         with st.expander(f"{row['name']} - ${row['price_per_night']}"):
    #             st.write(f"📍 Address: {row['address']}")
    #             st.write("♿ Wheelchair Accessible: ✅")
    #             if st.button(f"Notify staff at {row['name']}", key=row['name']):
    #                 success = notify_staff(row['name'], user_email)
    #                 if success:
    #                     st.success("Notification sent to staff.")
    #                 else:
    #                     st.error("Failed to send notification.")
