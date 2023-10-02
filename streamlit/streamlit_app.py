import streamlit as st

st.header('st.button')

if st.button('say hello'):
    st.write('hello there')

else:
    st.write('goodbye')