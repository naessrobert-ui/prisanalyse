import requests


def hent_regnskapsdata(org_nummer='912078957', aar=None, regnskapstype='SELSKAP'):
    base_url = f"https://data.brreg.no/regnskapsregisteret/regnskap/{org_nummer}"

    params = {}
    if aar:
        params['år'] = aar
    if regnskapstype:
        params['regnskapstype'] = regnskapstype

    try:
        response = requests.get(base_url, params=params, headers={'Accept': 'application/json'})
        response.raise_for_status()

        data = response.json()

        # Håndter både enkelt dict og liste
        if isinstance(data, list) and data:
            regnskap = data[0]
        elif isinstance(data, dict):
            regnskap = data
        else:
            print("Uventet responsformat fra API-et.")
            return

        # Grunnleggende info
        virksomhet = regnskap.get('virksomhet', {})
        print(f"Organisasjonsnummer: {virksomhet.get('organisasjonsnummer')}")
        periode = regnskap.get('regnskapsperiode', {})
        print(f"Regnskapsperiode: {periode.get('fraDato')} til {periode.get('tilDato')}")
        print(f"Valuta: {regnskap.get('valuta')}")
        print(f"Regnskapstype: {regnskap.get('regnskapstype')}")
        print(f"Oppstillingsplan: {regnskap.get('oppstillingsplan')}")

        # Hjelpefunksjon for å hente beløp trygt (håndterer både direkte tall og 'beloep')
        def get_belop(obj, *keys, default=0.0):
            current = obj
            for key in keys:
                if not isinstance(current, dict):
                    return default
                current = current.get(key, {})
            if isinstance(current, (int, float)):
                return float(current)
            if isinstance(current, dict):
                beloep = current.get('beloep')
                if beloep is not None:
                    return float(beloep)
            return default

        # Resultatregnskap
        resultat = regnskap.get('resultatregnskapResultat', {})
        drifts = resultat.get('driftsresultat', {})
        finans = resultat.get('finansresultat', {})

        sum_driftsinntekter = get_belop(drifts, 'driftsinntekter', 'sumDriftsinntekter')
        sum_driftskostnad = get_belop(drifts, 'driftskostnad', 'sumDriftskostnad')
        driftsresultat = get_belop(drifts, 'driftsresultat')

        sum_finansinntekter = get_belop(finans, 'finansinntekt', 'sumFinansinntekter')
        sum_finanskostnad = get_belop(finans, 'finanskostnad', 'sumFinanskostnad')
        netto_finans = get_belop(finans, 'nettoFinans')

        ord_resultat_foer_skatt = get_belop(resultat, 'ordinaertResultatFoerSkattekostnad')
        aarsresultat = get_belop(resultat, 'aarsresultat')
        totalresultat = get_belop(resultat, 'totalresultat')

        print("\n=== Resultatregnskap ===")
        print(f"  Sum driftsinntekter:          {sum_driftsinntekter:,.0f}")
        print(f"  Sum driftskostnader:          {sum_driftskostnad:,.0f}")
        print(f"  Driftsresultat:               {driftsresultat:,.0f}")
        print(f"  Sum finansinntekter:          {sum_finansinntekter:,.0f}")
        print(f"  Sum finanskostnader:          {sum_finanskostnad:,.0f}")
        print(f"  Netto finans:                 {netto_finans:,.0f}")
        print(f"  Ordinært resultat før skatt:  {ord_resultat_foer_skatt:,.0f}")
        print(f"  Årsresultat:                  {aarsresultat:,.0f}")
        print(f"  Totalresultat:                {totalresultat:,.0f}")

        # Balanse
        eiendeler = regnskap.get('eiendeler', {})
        sum_anleggsmidler = get_belop(eiendeler, 'anleggsmidler', 'sumAnleggsmidler')
        sum_omloepsmidler = get_belop(eiendeler, 'omloepsmidler', 'sumOmloepsmidler')
        sum_eiendeler = get_belop(eiendeler, 'sumEiendeler')

        egenkapital_gjeld = regnskap.get('egenkapitalGjeld', {})
        egenkapital = egenkapital_gjeld.get('egenkapital', {})
        gjeld = egenkapital_gjeld.get('gjeldOversikt', {})

        sum_egenkapital = get_belop(egenkapital, 'sumEgenkapital')
        sum_opptjent_ek = get_belop(egenkapital, 'opptjentEgenkapital', 'sumOpptjentEgenkapital')
        sum_innskutt_ek = get_belop(egenkapital, 'innskuttEgenkapital', 'sumInnskuttEgenkaptial')

        sum_langsiktig_gjeld = get_belop(gjeld, 'langsiktigGjeld', 'sumLangsiktigGjeld')
        sum_kortsiktig_gjeld = get_belop(gjeld, 'kortsiktigGjeld', 'sumKortsiktigGjeld')
        sum_gjeld = get_belop(gjeld, 'sumGjeld')
        sum_egenkapital_gjeld = get_belop(egenkapital_gjeld, 'sumEgenkapitalGjeld')

        print("\n=== Eiendeler ===")
        print(f"  Sum anleggsmidler:            {sum_anleggsmidler:,.0f}")
        print(f"  Sum omløpsmidler:             {sum_omloepsmidler:,.0f}")
        print(f"  Sum eiendeler:                {sum_eiendeler:,.0f}")

        print("\n=== Egenkapital og gjeld ===")
        print(f"  Sum egenkapital:              {sum_egenkapital:,.0f}")
        print(f"  Sum opptjent egenkapital:     {sum_opptjent_ek:,.0f}")
        print(f"  Sum innskutt egenkapital:     {sum_innskutt_ek:,.0f}")
        print(f"  Sum langsiktig gjeld:         {sum_langsiktig_gjeld:,.0f}")
        print(f"  Sum kortsiktig gjeld:         {sum_kortsiktig_gjeld:,.0f}")
        print(f"  Sum gjeld:                    {sum_gjeld:,.0f}")
        print(f"  Sum egenkapital + gjeld:      {sum_egenkapital_gjeld:,.0f}")

        # Beregn nøkkeltall
        print("\n=== Nøkkeltall ===")

        def safe_div(a, b, default=0.0):
            return a / b if b != 0 else default

        roe = safe_div(aarsresultat, sum_egenkapital) * 100
        driftsmargin = safe_div(driftsresultat, sum_driftsinntekter) * 100
        debt_to_equity = safe_div(sum_gjeld, sum_egenkapital)
        netto_finans_til_driftsres = safe_div((sum_finanskostnad - sum_finansinntekter), driftsresultat)

        soliditet = safe_div(sum_egenkapital, sum_eiendeler) * 100
        likviditetsgrad = safe_div(sum_omloepsmidler, sum_kortsiktig_gjeld)
        roa = safe_div(aarsresultat, sum_eiendeler) * 100
        interest_coverage = safe_div(driftsresultat, sum_finanskostnad)

        print(f"  Avkastning på egenkapital (ROE): {roe:.2f}%")
        print(f"  Driftsmargin:                    {driftsmargin:.2f}%")
        print(f"  Gjeld til egenkapital:           {debt_to_equity:.2f}")
        print(f"  Netto finanskostn. til driftsres:{netto_finans_til_driftsres:.2f}")
        print(f"  Soliditet (Equity Ratio):        {soliditet:.2f}%")
        print(f"  Likviditetsgrad (Current Ratio): {likviditetsgrad:.2f}")
        print(f"  Avkastning på eiendeler (ROA):   {roa:.2f}%")
        print(f"  Interest Coverage (approx.):     {interest_coverage:.2f}")

        # Lenke for PDF-nedlasting (manuell)
        pdf_url = f"https://w2.brreg.no/regnskap/sok.jsp?orgnr={org_nummer}"
        print("\n=== Nedlasting av full PDF-regnskap ===")
        print(f"Åpne denne lenken i nettleseren for å søke og laste ned PDF gratis: {pdf_url}")
        print("Velg regnskapsår og last ned derfra.")

    except requests.exceptions.RequestException as e:
        print(f"Feil ved API-kall: {e}")
    except Exception as e:
        print(f"Uventet feil: {e}")


# Eksempelkjøring
if __name__ == "__main__":
    print("Henter regnskap for 912078957 (siste år)")
    hent_regnskapsdata('912078957')  # Fjern aar for siste, eller sett aar=2024