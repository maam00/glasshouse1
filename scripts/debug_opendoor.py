#!/usr/bin/env python3
"""Debug script to extract Opendoor listings."""

import asyncio
from playwright.async_api import async_playwright
import json

async def extract_listings():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        print("Loading page...")
        await page.goto('https://www.opendoor.com/homes/phoenix-az', wait_until='domcontentloaded', timeout=60000)
        # Wait for listings to appear
        await page.wait_for_timeout(3000)
        await page.wait_for_selector('a[href*="/homes/"]', timeout=30000)

        # Scroll to load all
        print("Scrolling to load listings...")
        for _ in range(5):
            await page.evaluate('window.scrollBy(0, window.innerHeight)')
            await page.wait_for_timeout(1000)

        # First, let's see what's on the page - look for elements containing price text
        debug_info = await page.evaluate("""
        () => {
            const results = [];

            // Find elements that contain price patterns
            const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, null, false);
            const priceElements = [];

            while (walker.nextNode()) {
                const text = walker.currentNode.textContent;
                if (text && text.match(/\\$[\\d,]{5,}/)) {
                    let el = walker.currentNode.parentElement;
                    // Walk up to find the card container
                    for (let i = 0; i < 10 && el; i++) {
                        const tagName = el.tagName;
                        const className = el.className || '';
                        const hasLink = el.querySelector('a');

                        if (hasLink || className.toLowerCase().includes('card') || className.toLowerCase().includes('listing')) {
                            const link = el.querySelector('a');
                            const allText = el.innerText || '';
                            priceElements.push({
                                tagName: tagName,
                                className: className.substring(0, 100),
                                href: link ? link.getAttribute('href') : null,
                                text: allText.substring(0, 300),
                                level: i,
                            });
                            break;
                        }
                        el = el.parentElement;
                    }
                }
            }

            // Dedupe
            const seen = new Set();
            const unique = priceElements.filter(e => {
                const key = e.text.substring(0, 50);
                if (seen.has(key)) return false;
                seen.add(key);
                return true;
            });

            return unique.slice(0, 10);
        }
        """)
        print(f"\\nFound {len(debug_info)} card-like elements with prices:")
        for i, card in enumerate(debug_info[:5]):
            print(f"\\n  Card {i+1}:")
            print(f"    Tag: {card['tagName']}, Class: {card['className'][:60]}...")
            print(f"    Link: {card['href']}")
            print(f"    Text: {card['text'][:150]}...")
        print()

        # Extract listing data using JavaScript
        js_code = """
        () => {
            const results = [];

            // Find all links to property pages
            const links = document.querySelectorAll('a[href*="/properties/"]');

            links.forEach(link => {
                const href = link.getAttribute('href');
                if (!href) return;

                // Get the card container (parent elements)
                let card = link;
                for (let i = 0; i < 10 && card; i++) {
                    const text = card.innerText || '';
                    // Look for price pattern ($XXX,XXX)
                    const priceMatch = text.match(/\\$(\\d{1,3}(?:,\\d{3})+)/);
                    if (priceMatch) {
                        const bedMatch = text.match(/(\\d+)\\s*bds?/i);
                        const bathMatch = text.match(/(\\d+(?:\\.\\d+)?)\\s*ba/i);
                        const sqftMatch = text.match(/([\\d,]+)\\s*sqft/i);

                        // Extract city from URL (e.g., Goodyear from "Goodyear-AZ")
                        let city = null;
                        const urlCityMatch = href.match(/\\/properties\\/[^/]+-([A-Za-z-]+)-[A-Z]{2}-/);
                        if (urlCityMatch) {
                            city = urlCityMatch[1].replace(/-/g, ' ');
                        }

                        // Extract address from URL
                        let address = null;
                        const addrMatch = href.match(/\\/properties\\/([^/?]+)/);
                        if (addrMatch) {
                            address = addrMatch[1].replace(/-/g, ' ').replace(/\\s+(AZ|TX|FL|GA|NC|CO|NV|TN|CA|MN|OR|UT)\\s+\\d+.*$/i, '');
                        }

                        // Check for status
                        let status = 'FOR_SALE';
                        const textLower = text.toLowerCase();
                        if (textLower.includes('pending')) status = 'PENDING';
                        else if (textLower.includes('under contract')) status = 'UNDER_CONTRACT';
                        else if (textLower.includes('sold')) status = 'SOLD';

                        results.push({
                            url: href,
                            address: address,
                            price: parseInt(priceMatch[1].replace(/,/g, '')),
                            beds: bedMatch ? parseInt(bedMatch[1]) : null,
                            baths: bathMatch ? parseFloat(bathMatch[1]) : null,
                            sqft: sqftMatch ? parseInt(sqftMatch[1].replace(/,/g, '')) : null,
                            city: city,
                            status: status,
                            text_sample: text.substring(0, 200),
                        });
                        break;
                    }
                    card = card.parentElement;
                }
            });

            // Dedupe by URL
            const seen = new Set();
            return results.filter(r => {
                if (seen.has(r.url)) return false;
                seen.add(r.url);
                return true;
            });
        }
        """

        listings = await page.evaluate(js_code)

        print(f'Found {len(listings)} listings')
        print()
        for l in listings[:15]:
            status_marker = "🔵" if l['status'] == 'FOR_SALE' else "🟡" if l['status'] == 'PENDING' else "🟢"
            print(f"  {status_marker} {l['status']:15} ${l['price']:>10,} | {l.get('beds') or '?'}bd {l.get('baths') or '?'}ba {l.get('sqft') or '?'}sqft | {l.get('city') or 'Unknown'}")

        # Count by status
        statuses = {}
        for l in listings:
            statuses[l['status']] = statuses.get(l['status'], 0) + 1
        print(f'\nStatus breakdown: {statuses}')

        # Check for pending
        pending = [l for l in listings if l['status'] in ('PENDING', 'UNDER_CONTRACT')]
        print(f'\nPending/Under Contract: {len(pending)}')
        for p in pending[:5]:
            print(f"  {p['url']}")

        await browser.close()
        return listings

if __name__ == "__main__":
    asyncio.run(extract_listings())
