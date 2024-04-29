import streamlit as st
import pandas as pd
import numpy as np
import json
import requests
import streamlit_analytics


def main():
    streamlit_analytics.start_tracking()

    st.button("Reset", type="primary")
    if st.button('Say hello'):
        st.write('Why hello there')
    else:
        st.write('Goodbye')
    streamlit_analytics.stop_tracking()


    # sa.stop_tracking()

if __name__ == "__main__":
    main()
